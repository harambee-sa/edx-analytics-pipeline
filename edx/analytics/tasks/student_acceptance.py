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

SUBSECTION_PATTERN = r'/courses/[^/+]+(/|\+)[^/+]+(/|\+)[^/]+/courseware/[^/]+/[^/]+/.*$'

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
            log.error("encountered event with no event_type: %s", event)
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            log.error("encountered event with no course_id: %s", event)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            log.error("encountered event with no event_data: %s", event)
            return

        encoded_module_id = event_data.get('id')
        if encoded_module_id is None:
            log.error("encountered event with no encoded_module_id: %s", event)
            return

        if event_type[:9] == '/courses/' and re.match(SUBSECTION_PATTERN, event_type):
            event_type = 'subsection_viewed'
        else:
            return

        log.info("yielding event for: %s", encoded_module_id)

        yield ((course_id, encoded_module_id), (date_string))

    def reducer(self, key, events):
        """Calculate counts for events corresponding to course and (sub)section in a given time period."""
        course_id, encoded_module_id = key
        num_views = len(events)

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            course_id.encode('utf-8'),
            encoded_module_id.encode('utf8'),
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
            ('encoded_module_id', 'STRING'),
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