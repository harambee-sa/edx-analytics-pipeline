"""Measure student acceptance across (sub)sections in the course."""

import re
import logging
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.util.hive import HivePartition, HiveTableTask
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util import eventlog

log = logging.getLogger(__name__)

SUBSECTION_PATTERN = r'/courses/[^/+]+(/|\+)[^/+]+(/|\+)[^/]+/courseware$'

class StudentAcceptanceDataTask(EventLogSelectionMixin, MapReduceJobTask):
    """Capture last student acceptance for a given interval"""
    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        event_type = event.get('event_type')
        if event_type is None:
            return

        if event_type[:9] == '/courses/' and re.match(SUBSECTION_PATTERN, event_type):
            event_type = 'page_view'
        else:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_context = event.get('context')
        if event_context is None:
            return

        path = event_context.get('path', None)
        if path is None:
            log.error("encountered page_view event with no path: %s", event)
            return

        log.info("yielding mapped record for: %s", event)

        yield ((course_id, path), (date_string))

    def reducer(self, key, events):
        """Calculate counts for events corresponding to course and (sub)section in a given time period."""
        course_id, path = key
        num_views = len(events)

        log.info("yielding reduced record for events: %s", events)

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            course_id.encode('utf-8'),
            path.encode('utf8'),
            num_views
        )

    def output(self):
        return get_target_from_url(self.output_root)


class StudentAcceptanceTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, HiveTableTask):
    """Hive table that stores the count of subsection views in each course over time."""

    @property
    def table(self):
        return 'student_acceptance'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('path', 'STRING'),
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