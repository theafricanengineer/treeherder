import os
from hashlib import sha1
import time
import urllib2
from django.conf import settings
from datasource.bases.BaseHub import BaseHub
from datasource.DataHub import DataHub


class RefDataManager(object):
    """Model for reference data"""

    def __init__(self):
        procs_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'sql', 'reference.json')
        data_source = {
            'reference': {
                "hub": "MySQL",
                "master_host": {
                    "host": settings.DATABASES['default']['HOST'],
                    "user": settings.DATABASES['default']['USER'],
                    "passwd": settings.DATABASES['default']['PASSWORD']
                },
                "default_db": settings.DATABASES['default']['NAME'],
                "procs": [procs_path]
            }
        }
        BaseHub.add_data_source(data_source)
        self.dhub = DataHub.get("reference")
        self.DEBUG = settings.DEBUG

        # Support structures for building build platform SQL
        self.build_platform_lookup = {}
        self.build_where_filters = []
        self.build_platform_placeholders = []
        self.build_unique_platforms = []

        # Support structures for building machine platform SQL
        self.machine_platform_lookup = {}
        self.machine_where_filters = []
        self.machine_platform_placeholders = []
        self.machine_unique_platforms = []

        # Support structures for building job group SQL
        self.job_group_lookup = set()
        self.job_group_where_in_list = []
        self.job_group_placeholders = []
        self.unique_job_groups = []

        # Support structures for building job types SQL
        self.job_type_lookup = set()
        self.job_type_where_in_list = []
        self.job_type_placeholders = []
        self.unique_job_types = []

        # Support structures for building product SQL
        self.product_lookup = set()
        self.product_where_in_list = []
        self.product_placeholders = []
        self.unique_products = []

        # Support structures for building machine SQL
        self.machine_name_lookup = set()
        self.machine_where_in_list = []
        self.machine_name_placeholders = []
        self.machine_unique_names = []
        self.machine_timestamp_update_placeholders = []

        # Support structures for building option collection data structures
        self.oc_hash_lookup = dict()
        self.oc_where_in_list = []
        self.oc_placeholders = []
        self.oc_unique_collections = []

        # Support structures for building option data structures
        self.o_lookup = set()
        self.o_placeholders = []
        self.o_unique_options = []
        self.o_where_in_list = []

    def disconnect(self):
        self.dhub.disconnect()

    def set_all_reference_data(self):
        """This method executes SQL to store data in all loaded reference
           data structures. It returns lookup dictionaries where the key is
           typically the string provided to the data structure and the value
           includes the database id associated with it. Once all of the
           reference data is processed, the reference data structures are
           initialized to empty structures so the same class instance can be
           used to process more reference data if necessary.

           In general, users of this class should first iterate through job
           data, calling appropriate add* class instance methods to load the
           reference data once all of the data is loaded, call this method
           to process the data.
        """

        # id lookup structure
        self.id_lookup = {
            'build_platforms':self.process_build_platforms(),
            'machine_platforms':self.process_machine_platforms(),

            'job_groups':self.process_job_groups(),
            'job_types':self.process_job_types(),
            'products':self.process_products(),

            'machines':self.process_machines(),
            'option_collections':self.process_option_collections()
            }

        self.reset_reference_data()

        return self.id_lookup

    def reset_reference_data(self):
        """Reset all reference data structures, this should be called after
           processing data.
        """

        # reset build platforms
        self.build_platform_lookup = {}
        self.build_where_filters = []
        self.build_platform_placeholders = []
        self.build_unique_platforms = []

        # reset machine platforms
        self.machine_platform_lookup = {}
        self.machine_where_filters = []
        self.machine_platform_placeholders = []
        self.machine_unique_platforms = []

        # reset job groups
        self.job_group_lookup = set()
        self.job_group_where_in_list = []
        self.job_group_placeholders = []
        self.unique_job_groups = []

        # reset job types
        self.job_type_lookup = set()
        self.job_type_where_in_list = []
        self.job_type_placeholders = []
        self.unique_job_types = []

        # reset products
        self.product_lookup = set()
        self.product_where_in_list = []
        self.product_placeholders = []
        self.unique_products = []

        # reset machines
        self.machine_name_lookup = set()
        self.machine_where_in_list = []
        self.machine_name_placeholders = []
        self.machine_unique_names = []
        self.machine_timestamp_update_placeholders = []

        # reset option collections
        self.oc_hash_lookup = dict()
        self.oc_where_in_list = []
        self.oc_placeholders = []
        self.oc_unique_collections = []

        # reset options
        self.o_lookup = set()
        self.o_placeholders = []
        self.o_unique_options = []
        self.o_where_in_list = []

    """
    Collection of add_* methods that take some kind of reference
    data and populate a set of class instance data structures. These
    methods allow a caller to iterate through a single list of
    job data structures, generating cumulative sets of reference data.
    """
    def add_build_platform(self, os_name, platform, arch):
        """
        Add build platform reference data. Requires an
        operating system name, platform designator, and architecture
        type.

        os_name - linux | mac | win | Android | Firefox OS | ...
        platform - fedora 12 | redhat 12 | 5.1.2600 | 6.1.7600 | OS X 10.7.2 | ...
        architecture - x86 | x86_64 etc...
        """

        key = self._add_platform(
            os_name, platform, arch,
            self.build_platform_lookup,
            self.build_platform_placeholders,
            self.build_unique_platforms,
            self.build_where_filters
            )

        return key

    def add_machine_platform(self, os_name, platform, arch):
        """
        Add machine platform reference data. Requires an
        operating system name, platform designator, and architecture
        type.

        os_name - linux | mac | win | Android | Firefox OS | ...
        platform - fedora 12 | redhat 12 | 5.1.2600 | 6.1.7600 | OS X 10.7.2 | ...
        architecture - x86 | x86_64 etc...
        """

        key = self._add_platform(
            os_name, platform, arch,
            self.machine_platform_lookup,
            self.machine_platform_placeholders,
            self.machine_unique_platforms,
            self.machine_where_filters
            )

        return key

    def add_job_group(self, group):
        """Add job group names"""

        self._add_name(
            group, self.job_group_lookup, self.job_group_placeholders,
            self.unique_job_groups, self.job_group_where_in_list
            )

    def add_job_type(self, job_type):
        """Add job type names"""

        self._add_name(
            job_type, self.job_type_lookup, self.job_type_placeholders,
            self.unique_job_types, self.job_type_where_in_list
            )

    def add_product(self, product):
        """Add product names"""

        self._add_name(
            product, self.product_lookup, self.product_placeholders,
            self.unique_products, self.product_where_in_list
            )

    def _add_platform(
        self,
        os_name, platform, arch,
        platform_lookup,
        platform_placeholders,
        unique_platforms,
        where_filters):
        """
        Internal method for adding platform information, the platform
        could be a build or machine platform. The caller must provide
        the appropriate instance data structures as arguments.
        """

        key = RefDataManager.get_platform_key(os_name, platform, arch)

        if key not in platform_lookup:

            # Placeholders for the INSERT/SELECT SQL query
            platform_placeholders.append(
                [ os_name, platform, arch, os_name, platform, arch ]
                )

            # Placeholders for the id retrieval SELECT
            unique_platforms.extend(
                [ os_name, platform, arch ]
                )

            # Initializing return data structure
            platform_lookup[key] = {
                'id':0,
                'os_name':os_name,
                'platform':platform,
                'architecture':arch
                }

            # WHERE clause for the retrieval SELECT
            where_filters.append(
                "(`os_name` = %s  AND `platform` = %s  AND `architecture` = %s)".format(
                    os_name, platform, arch
                    )
                )

        return key

    def _add_name(
        self, name, name_lookup, name_placeholders, unique_names,
        where_in_list):
        """
        Internal method for adding reference data that consists of a single
        name. The caller must provide the appropriate instance data
        structures as arguments.
        """
        name_lookup.add(name)

        # Placeholders for the INSERT/SELECT SQL query
        name_placeholders.append(
            [ name, name ]
            )

        # Placeholders for the id retrieval SELECT
        unique_names.append( name )

        # WHERE clause for the retrieval SELECT
        where_in_list.append('%s')

    def add_machine(self, machine_name, timestamp):
        """
        Add machine name and timestamp. There are two timestamps stored in
        the database for each machine, one associated with the first time
        the machine is seen and another that acts as a heartbeat for the
        machine.
        """

        if machine_name not in self.machine_name_lookup:

            self.machine_name_lookup.add(machine_name)

            # Placeholders for the INSERT/SELECT SQL query
            self.machine_name_placeholders.append(
                # machine_name, first_timestamp, last_timestamp,
                # machine_name
                [ machine_name, timestamp, timestamp, machine_name ]
                )

            # Placeholders for the id retrieval SELECT
            self.machine_unique_names.append( machine_name )

            # WHERE clause for the retrieval SELECT
            self.machine_where_in_list.append('%s')

            # NOTE: It's possible that the same machine occurs
            #   multiple times in names_and_timestamps with different
            #   timestamps. We're assuming those timestamps will be
            #   reasonably close to each other and the primary intent
            #   of storing the last_timestamp is to keep track of the
            #   approximate time a particular machine last reported.
            self.machine_timestamp_update_placeholders.append(
                [timestamp, machine_name]
                )

    def add_option_collection(self, option_set):
        """
        Add an option collection. An option collection is made up of a
        set of options. Each unique set of options is hashed, this hash
        becomes the identifier for the option set. Options are stored
        individually in the database, callers only interact directly with
        sets of options, even when there's only on option in a set.
        """

        # New set with elements in option_set but not in o_lookup
        new_options = set(option_set) - self.o_lookup

        if new_options:
            # Extend o_lookup with new options
            self.o_lookup = self.o_lookup.union(new_options)

            for o in new_options:
                # Prepare data structures for option insertion
                self.o_placeholders.append([o, o])
                self.o_unique_options.append(o)
                self.o_where_in_list.append('%s')

        option_collection_hash = self.get_option_collection_hash(
             option_set
            )

        if option_collection_hash not in self.oc_hash_lookup:
            # Build list of unique option collections
            self.oc_hash_lookup[option_collection_hash] = option_set

        return option_collection_hash

    """
    The following set of process_* methods carry out the task
    of SQL generation and execution using the class instance reference
    data structures.
    """
    def process_build_platforms(self):
        """
        Process the build platform reference data
        """

        insert_proc = 'reference.inserts.create_build_platform'
        select_proc = 'reference.selects.get_build_platforms'

        return self._process_platforms(
            insert_proc, select_proc,
            self.build_platform_lookup,
            self.build_platform_placeholders,
            self.build_unique_platforms,
            self.build_where_filters
            )

    def process_machine_platforms(self):
        """
        Process the machine platform reference data
        """

        insert_proc = 'reference.inserts.create_machine_platform'
        select_proc = 'reference.selects.get_machine_platforms'

        return self._process_platforms(
            insert_proc, select_proc,
            self.machine_platform_lookup,
            self.machine_platform_placeholders,
            self.machine_unique_platforms,
            self.machine_where_filters
            )

    def process_job_groups(self):
        """
        Process the job group reference data
        """

        insert_proc = 'reference.inserts.create_job_group'
        select_proc='reference.selects.get_job_groups'

        return self._process_names(
            insert_proc, select_proc,
            self.job_group_where_in_list,
            self.job_group_placeholders,
            self.unique_job_groups
            )

    def process_job_types(self):
        """
        Process the job type reference data
        """

        insert_proc = 'reference.inserts.create_job_type'
        select_proc='reference.selects.get_job_types'

        return self._process_names(
            insert_proc, select_proc,
            self.job_type_where_in_list,
            self.job_type_placeholders,
            self.unique_job_types
            )

    def process_products(self):
        """
        Process the product reference data
        """

        insert_proc = 'reference.inserts.create_product'
        select_proc='reference.selects.get_products'

        return self._process_names(
            insert_proc, select_proc,
            self.product_where_in_list,
            self.product_placeholders,
            self.unique_products
            )

    def process_machines(self):
        """
        Process the machine reference data
        """

        if not self.machine_name_placeholders:
            return {}

        # Convert WHERE filters to string
        where_in_clause = ",".join(self.machine_where_in_list)

        select_proc='reference.selects.get_machines'
        insert_proc = 'reference.inserts.create_machine'
        update_proc = 'reference.updates.update_machine_timestamp'

        self.dhub.execute(
            proc=insert_proc,
            placeholders=self.machine_name_placeholders,
            executemany=True,
            debug_show=self.DEBUG)

        name_lookup = self.dhub.execute(
            proc=select_proc,
            placeholders=self.machine_unique_names,
            replace=[where_in_clause],
            key_column='name',
            return_type='dict',
            debug_show=self.DEBUG)

        """
        There is a bug in the python mysqldb module that is triggered by the
        use of an INSERT/SELECT/ON DUPLICATE KEY query with the executemany
        option that results in

        'TypeError: not all arguments converted during string formatting'

        To circumvent this we do an explicit update to set the
        last_timestamp. In parallel job execution this could lead to a
        race condition where the machine timestamp is set by another
        job processor but the intention of last_timestamp is to keep an
        approximate time associated with the machine's last report so this
        should not be a problem.

        NOTE: There was a possibility of a data integrity issue caused by the
            ON DUPLICATE KEY UPDATE strategy. When the ON DUPLICATE KEY clause
            is executed the auto increment id will be incremented. This has
            the potential to mangle previous stored machine_ids. This would
            be bad...
        """
        self.dhub.execute(
            proc=update_proc,
            placeholders=self.machine_timestamp_update_placeholders,
            executemany=True,
            debug_show=self.DEBUG)

        return name_lookup

    def process_option_collections(self):
        """
        Process option collection data
        """

        # Store options not seen yet
        o_where_in_clause = ",".join(self.o_where_in_list)
        option_id_lookup = self._get_or_create_options(
            self.o_placeholders, self.o_unique_options, o_where_in_clause
            )

        # Get the list of option collection placeholders
        for oc_hash in self.oc_hash_lookup:
            for o in self.oc_hash_lookup[oc_hash]:
                self.oc_placeholders.append([
                    oc_hash, option_id_lookup[o]['id'], oc_hash,
                    option_id_lookup[o]['id']
                    ])

        if not self.oc_placeholders:
            return {}

        self.dhub.execute(
            proc='reference.inserts.create_option_collection',
            placeholders=self.oc_placeholders,
            executemany=True,
            debug_show=self.DEBUG)

        return self.oc_hash_lookup

    def _process_platforms(
        self, insert_proc, select_proc, platform_lookup,
        platform_placeholders, unique_platforms, where_filters):
        """
        Internal method for processing either build or machine platforms.
        The caller is required to provide the appropriate data structures
        depending on what type of platform is being processed.
        """

        if where_filters:

            self.dhub.execute(
                proc=insert_proc,
                placeholders=platform_placeholders,
                executemany=True,
                debug_show=self.DEBUG)

            # Convert WHERE filters to string
            where_in_clause = " OR ".join(where_filters)

            # NOTE: This query is using master_host to insure we don't have a
            # race condition with INSERT into master and SELECT new ids from
            # the slave.
            data_retrieved = self.dhub.execute(
                proc=select_proc,
                placeholders=unique_platforms,
                replace=[where_in_clause],
                debug_show=self.DEBUG)

            for data in data_retrieved:

                key = RefDataManager.get_platform_key(
                    data['os_name'], data['platform'], data['architecture']
                    )

                platform_lookup[key]['id'] = int(data['id'])

        return platform_lookup

    def _process_names(
        self, insert_proc, select_proc, where_in_list, name_placeholders,
        unique_names):
        """
        Internal method for processing reference data names. The caller is
        required to provide the appropriate data structures for the target
        reference data type.
        """

        if not name_placeholders:
            return {}

        # Convert WHERE filters to string
        where_in_clause = ",".join(where_in_list)

        self.dhub.execute(
            proc=insert_proc,
            placeholders=name_placeholders,
            executemany=True,
            debug_show=self.DEBUG)

        name_lookup = self.dhub.execute(
            proc=select_proc,
            placeholders=unique_names,
            replace=[where_in_clause],
            key_column='name',
            return_type='dict',
            debug_show=self.DEBUG)

        return name_lookup

    def get_or_create_build_platforms(self, platform_data):
        """
        Get or create build platforms for a list of platform data.
        See _get_or_create_platforms for data structure descriptions.
        """

        insert_proc = 'reference.inserts.create_build_platform'
        select_proc = 'reference.selects.get_build_platforms'

        return self._get_or_create_platforms(
            platform_data, insert_proc, select_proc,
            self.build_platform_lookup,
            self.build_platform_placeholders,
            self.build_unique_platforms,
            self.build_where_filters
            )

    def get_or_create_machine_platforms(self, platform_data):
        """
        Get or create machine platforms for a list of platform data.
        See _get_or_create_platforms for data structure descriptions.
        """

        insert_proc = 'reference.inserts.create_machine_platform'
        select_proc = 'reference.selects.get_machine_platforms'

        return self._get_or_create_platforms(
            platform_data, insert_proc, select_proc,
            self.machine_platform_lookup,
            self.machine_platform_placeholders,
            self.machine_unique_platforms,
            self.machine_where_filters
            )

    def _get_or_create_platforms(
        self, platform_data, insert_proc, select_proc,
        platform_lookup, platform_placeholders, unique_platforms,
        where_filters):
        """
        Takes a list of lists of os_name, platform, and architecture
        columns and returns a dictionary to be used as a lookup for each
        combination's associated id. Any platforms not found are created,
        duplicate platforms are aggregated to minimize database operations.

        platform_data =
            [
                [os_name, platform, architecture],
                [os_name, platform, architecture],
                ...
                ]

        returns {
            "os_name-platform-architecture": {
                id:id, os_name:os_name,
                platform:platform,
                architecture:architecture
                },
            "os_name-platform-architecture": {
                id:id,
                os_name:os_name,
                platform:platform,
                architecture:architecture
                },
            ...
            }
        """
        for item in platform_data:

            self._add_platform(
                #os_name, platform, architecture
                item[0], item[1], item[2],
                platform_lookup, platform_placeholders,
                unique_platforms, where_filters
                )

        return self._process_platforms(
            insert_proc, select_proc,
            platform_lookup,
            platform_placeholders,
            unique_platforms,
            where_filters
            )

    @classmethod
    def get_platform_key(cls, os_name, platform, architecture):
        return "{0}-{1}-{2}".format(os_name, platform, architecture)

    def get_or_create_job_groups(self, names):
        """
        Get or create job groups given a list of job group names.
        See _get_or_create_names for data structure descriptions.
        """

        insert_proc = 'reference.inserts.create_job_group'
        select_proc='reference.selects.get_job_groups'

        return self._get_or_create_names(
                    names, insert_proc, select_proc,
                    self.job_group_lookup, self.job_group_placeholders,
                    self.unique_job_groups, self.job_group_where_in_list)

    def get_or_create_job_types(self, names):
        """
        Get or create job types given a list of job type names.
        See _get_or_create_names for data structure descriptions.
        """

        insert_proc = 'reference.inserts.create_job_type'
        select_proc='reference.selects.get_job_types'

        return self._get_or_create_names(
                    names, insert_proc, select_proc,
                    self.job_type_lookup, self.job_type_placeholders,
                    self.unique_job_types, self.job_type_where_in_list)

    def get_or_create_products(self, names):
        """
        Get or create products given a list of product names.  See
        _get_or_create_names for data structure descriptions.
        """

        insert_proc = 'reference.inserts.create_product'
        select_proc='reference.selects.get_products'

        return self._get_or_create_names(
                    names, insert_proc, select_proc,
                    self.product_lookup, self.product_placeholders,
                    self.unique_products, self.product_where_in_list)

    def get_or_create_machines(self, names_and_timestamps):
        """
        Takes a list of machine names and timestamps returns a dictionary to
        be used as a lookup for each machine name's id. Any names not found
        are inserted into the appropriate table, duplicate machine names are
        aggregated to minimize database operations.

        names = [
            [ machine1, time1 ],
            [ machine2, time2 ],
            [ machine3, time3 ],
            ... ]

        returns {
            'machine1':{'id':id, 'name':name },
            'machine1':{'id':id, 'name':name },
            'machine1':{'id':id, 'name':name },
            ...
            }
        """
        for item in names_and_timestamps:
            # machine name, timestamp
            self.add_machine(item[0], item[1])

        return self.process_machines()

    def _get_or_create_names(self,
        names, insert_proc, select_proc,
        name_lookup, where_in_list, name_placeholders, unique_names):
        """
        Takes a list of names and returns a dictionary to be used as a
        lookup for each name's id. Any names not found are inserted into
        the appropriate table, duplicate platforms are aggregated to
        minimize database operations.

        names = [ name1, name2, name3 ... ]

        returns { 'name1':id, 'name2':id, 'name3':id, ... }
        """
        for name in names:
            self._add_name(
                name, name_lookup, name_placeholders,
                unique_names, where_in_list
                )

        return self._process_names(
            insert_proc, select_proc, where_in_list, name_placeholders,
            unique_names
            )


    def get_option_collection_hash(self, options):
        """returns an option_collection_hash given a list of options"""

        options = sorted(list(options))
        sha_hash = sha1()
        # equivalent to loop over the options and call sha_hash.update()
        sha_hash.update(''.join(options))
        return sha_hash.hexdigest()

    def get_or_create_option_collection(self, option_collections):
        """
        Get or create option collections for each list of options provided.

        [
            [ option1, option2, option3 ],
            ...
        ]
        """

        # Build set of unique options
        for option_set in option_collections:

            self.add_option_collection(option_set)

        return self.process_option_collections()

    def _get_or_create_options(
        self, option_placeholders, unique_options, where_in_clause):

        if not option_placeholders:
            return {}

        insert_proc = 'reference.inserts.create_option'
        select_proc='reference.selects.get_options'

        self.dhub.execute(
            proc=insert_proc,
            placeholders=option_placeholders,
            executemany=True,
            debug_show=self.DEBUG)

        option_lookup = self.dhub.execute(
            proc=select_proc,
            placeholders=unique_options,
            replace=[where_in_clause],
            key_column='name',
            return_type='dict',
            debug_show=self.DEBUG)

        return option_lookup

    def get_db_name(self):
        """The name of the database holding the refdata tables"""
        return self.dhub.conf["default_db"]

    def get_row_by_id(self, table_name, obj_id):
        iter_obj = self.dhub.execute(
            proc="reference.selects.get_row_by_id",
            replace=[table_name],
            placeholders=[obj_id],
            debug_show=self.DEBUG,
            return_type='iter',
        )

        return iter_obj

    def get_all_option_collections(self):
        """
        Returns all option collections in the following data structure

        {
            "hash1":{
                option_collection_hash : "hash1",
                opt:"opt1 opt2"
                },
            "hash2":{
                option_collection_hash : "hash2",
                opt:"opt3 opt4 opt5"
                }
            ...
            }
        """
        return self.dhub.execute(
            proc='reference.selects.get_all_option_collections',
            debug_show=self.DEBUG,
            key_column='option_collection_hash',
            return_type='dict'
        )


    def get_repository_id(self, name):
        """get the id for the given repository"""

        id_iter = self.dhub.execute(
            proc='reference.selects.get_repository_id',
            placeholders=[name],
            debug_show=self.DEBUG,
            return_type='iter')

        return id_iter.get_column_data('id')

    def get_repository_version_id(self, repository_id):
        """get the latest version available for the given repository"""

        id_iter = self.dhub.execute(
            proc='reference.selects.get_repository_version_id',
            placeholders=[repository_id],
            debug_show=self.DEBUG,
            return_type='iter')

        return id_iter.get_column_data('id')

    def get_or_create_repository_version(self, repository_id, version,
                                         version_timestamp):

        self.dhub.execute(
            proc='reference.inserts.create_repository_version',
            placeholders=[
                repository_id,
                version,
                version_timestamp,
                repository_id,
                version
            ],
            debug_show=self.DEBUG)

        return self.get_repository_version_id(repository_id)

    def update_repository_version(self, repository_id):
        """update repository version with the latest information
        avaliable. the only dvcs supported is hg"""

        repository = self.get_repository_info(repository_id)

        if repository['dvcs_type'] != 'hg':
            raise NotImplementedError
        else:
            version = self.get_hg_repository_version(repository['url'])

        timestamp_now = time.time()

        # try to create a new repository version
        self.get_or_create_repository_version(repository_id,
                                              version, timestamp_now)

        # update the version_timestamp
        self.dhub.execute(
            proc='reference.updates.update_version_timestamp',
            placeholders=[
                timestamp_now,
                repository_id,
                version
            ],
            debug_show=self.DEBUG)

    def get_hg_repository_version(self, repo_url):
        """retrieves the milestone.txt file used to indicate
        the current milestone of a repo. the last line contains
        the info needed"""

        milestone_path = '/raw-file/default/config/milestone.txt'
        version_url = "".join((repo_url, milestone_path))

        response = urllib2.urlopen(version_url)
        for line in response:
            #go to the last line
            pass
        return line.strip()

    def get_repository_info(self, repository_id):
        """retrieves all the attributes of a repository"""

        repo = self.dhub.execute(
            proc='reference.selects.get_repository_info',
            placeholders=[repository_id],
            debug_show=self.DEBUG,
            return_type='iter')
        # retrieve the first elem from DataIterator
        for r in repo:
            return r

    def get_all_repository_info(self):
        return self.dhub.execute(
            proc='reference.selects.get_all_repository_info',
            debug_show=self.DEBUG,
            return_type='iter')


    def update_bugscache(self, bug_list):
        """
        Add content to the bugscache, updating/inserting
        when necessary.
        """
        placeholders = []
        # create a list of placeholders from a list of dictionary
        for bug in bug_list:
            # keywords come as a list of values, we need a string instead
            bug['keywords'] = ",".join(bug['keywords'])
            placeholders.append([bug[field] for field in (
                    'id', 'status', 'resolution', 'summary',
                    'cf_crash_signature', 'keywords', 'op_sys', 'id')])

        self.dhub.execute(
            proc='reference.inserts.create_bugscache',
            placeholders=placeholders,
            executemany=True,
            debug_show=self.DEBUG)

        # removing the first placeholder because is not used in the update query
        del placeholders[0]

        self.dhub.execute(
            proc='reference.updates.update_bugscache',
            placeholders=placeholders,
            executemany=True,
            debug_show=self.DEBUG)






