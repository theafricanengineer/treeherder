from django.conf import settings
from django.core.management.base import BaseCommand

from treeherder.model.models import FailureLine


class Command(BaseCommand):
    help = """Find an remove orphaned data that have no jobs"""

    def add_arguments(self, parser):
        parser.add_argument(
            '--debug',
            action='store_true',
            dest='debug',
            default=False,
            help='Write debug messages to stdout'
        )
        parser.add_argument(
            '--chunk-size',
            action='store',
            dest='chunk_size',
            default=settings.DATA_CYCLE_CHUNK_SIZE,
            type=int,
            help=('Define the size of the chunks '
                  'Split the job deletes into chunks of this size')
        )

    def handle(self, *args, **options):
        self.is_debug = options['debug']

        self.debug("chunk size... {}".format(options['chunk_size']))

        self.clean_orphaned_failure_lines(options['chunk_size'])

    def clean_orphaned_failure_lines(self, chunk_size):
        results = FailureLine.objects.raw(
            """
                SELECT failure_line.job_guid, failure_line.id
                FROM treeherder.failure_line
                INNER JOIN (
                    SELECT fl.id
                    FROM treeherder.failure_line AS fl
                    LEFT JOIN treeherder.job ON job.guid=fl.job_guid
                    WHERE job.guid IS NULL
                    ORDER BY fl.id
                    LIMIT {chunk_size}
                    ) jobber
                ON jobber.id = failure_line.id;
            """.format(chunk_size=chunk_size))
        guids = {fl.job_guid for fl in results}
        FailureLine.objects.filter(job_guid__in=guids).delete()

    def debug(self, msg):
        if self.is_debug:
            self.stdout.write(msg)
