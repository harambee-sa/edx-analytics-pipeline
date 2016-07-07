"""Measure student acceptance across (sub)sections in the course."""

import re
import logging
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.util.hive import HivePartition, HiveTableTask, HiveQueryToMysqlTask
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util import eventlog

log = logging.getLogger(__name__)

# /courses/course-v1:RAP+RA001+2016/courseware/a314922c8770494789e139e7e63e7aa2/96f9add9dd7546a79f801ecf79fc1798/
SUBSECTION_PATTERN = r'/courses/[^/+]+(/|\+)[^/+]+(/|\+)[^/]+/courseware/[^/]+/[^/]+/.*$'

class StudentAcceptanceDataTask(EventLogSelectionMixin, MapReduceJobTask):
    """Capture last student acceptance for a given interval"""

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        event_source = event.get('event_source')
        if event_source != 'server':
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        username = event.get('username').strip()
        if not username:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        path = event_type
        if event_type[:9] == '/courses/' and re.match(SUBSECTION_PATTERN, event_type):
            event_type = 'page_view'
        else:
            return

        breadcrumbs = path.strip('/').strip().split('/')
        unit = ((breadcrumbs[5] or '0') if len(breadcrumbs) == 6 else '0')

        yield ((course_id, breadcrumbs[3], breadcrumbs[4], unit, username), (date_string))

    def reducer(self, key, events):
        """Calculate counts for events corresponding to course and (sub)section in a given time period."""

        course_id, section, subsection, unit, username = key
        num_views = len(list(events))

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            course_id.encode('utf-8'),
            section.encode('utf8'),
            subsection.encode('utf8'),
            unit.encode('utf8'),
            username.encode('utf8'),
            num_views
        )

    def output(self):
        return get_target_from_url(self.output_root)


class StudentAcceptanceTableTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, HiveTableTask):
    """Hive table that stores the count of subsection views in each course over time."""

    @property
    def table(self):
        return 'student_acceptance'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('section', 'STRING'),
            ('subsection', 'STRING'),
            ('unit', 'STRING'),
            ('username', 'STRING'),
            ('num_views', 'INT')
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return StudentAcceptanceDataTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location
        )


class StudentAcceptanceTask(HiveQueryToMysqlTask):
    """Calculate unique vs total views and insert into MySQL."""

    interval = luigi.DateIntervalParameter(
        description='The range of dates to import logs for.',
        default=None
    )

    @property
    def table(self):
        return 'student_acceptance'

    columns = [
        ('course_id', 'VARCHAR'),
        ('section', 'VARCHAR'),
        ('subsection', 'VARCHAR'),
        ('unit', 'VARCHAR'),
        ('num_unique_views', 'INT'),
        ('num_views', 'INT')
    ]

    @property
    def auto_primary_key(self):
        # Instead of using an auto incrementing primary key, we define a custom compound primary key. See keys() defined
        # below. This vastly improves the performance of our most common query pattern.
        return None

    @property
    def keys(self):
        """
        Combine mulitple fields that must be unique together as the primary key for this table.
        This dramatically speeds up access times at the cost of write speed.
        """
        # max length for this key must be under 3072 bytes; see comment in module_engagement.py
        return [
            ('PRIMARY KEY', ['course_id', 'section', 'subsection', 'unit'])
        ]

    @property
    def required_table_tasks(self):
        return StudentAcceptanceTableTask(
            interval=self.interval
        )

    @property
    def query(self):
        return """
            SELECT course_id, section, subsection, unit, COUNT(*) AS num_unique_views, SUM(num_views)
            FROM student_acceptance GROUP BY course_id, section, subsection, unit
        """

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member