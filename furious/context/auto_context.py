#
# Copyright 2012 WebFilings, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Furious AutoContext is used batch inserts of tasks into groups.

It is similar to Context, but inserts automatically before the context
is exited.
"""


from .context import Context
from .context import _task_batcher
from .task_futures import _future_tasks_get_result
from .task_futures import _insert_tasks_async

from .. import errors


class AutoContext(Context):
    """Similar to context, but automatically inserts tasks asynchronously as
    they are added to the context.  Inserted in batches if specified.
    """

    def __init__(self, batch_size=None, async_insert=False, **options):
        """Setup this context in addition to accepting a batch_size."""

        Context.__init__(self, **options)

        self.batch_size = batch_size
        self.async_insert = async_insert

        self._insert_tasks_async = options.pop(
            'insert_tasks_async', _insert_tasks_async)
        if not callable(self._insert_tasks_async):
            raise TypeError(
                'You must provide a valid insert_tasks_async function.')

        # Futures representing async tasks.
        self.task_futures_info = []

    def add(self, target, args=None, kwargs=None, **options):
        """Add an Async job to this context.

        Like Context.add(): creates an Async and adds it to our list of tasks.
        but also calls _auto_insert_check() to add tasks to queues
        automatically.
        """

        # In superclass, add new task to our list of tasks
        target = super(
            AutoContext, self).add(target, args, kwargs, **options)

        self._auto_insert_check()

        return target

    def _auto_insert_check(self):
        """Automatically insert tasks asynchronously.
        Depending on batch_size, insert or wait until next call.
        """

        if not self.batch_size:
            return

        if len(self._tasks) >= self.batch_size:
            self._handle_tasks()

    def _handle_tasks_insert(self, batch_size=None):
        """Convert all Async's into tasks, then insert them into queues."""
        if self._tasks_inserted:
            raise errors.ContextAlreadyStartedError(
                "This Context has already had its tasks inserted.")

        task_map = self._get_tasks_by_queue()
        for queue, tasks in task_map.iteritems():
            for batch in _task_batcher(tasks, batch_size=batch_size):
                if self.async_insert:
                    self.task_futures_info.append(
                        self._insert_tasks_async(batch, queue=queue))
                else:
                    self._insert_tasks(batch, queue=queue)

    def _handle_tasks(self):
        """Convert Async's into tasks, then insert them into queues.
        Similar to the default _handle_tasks, but don't mark all
        tasks inserted.
        """

        self._handle_tasks_insert(batch_size=self.batch_size)
        self._tasks = []

    def __exit__(self, exc_type, exc_val, exc_tb):
        """In addition to the default __exit__(), also mark all tasks
        inserted.
        """

        super(AutoContext, self).__exit__(exc_type, exc_val, exc_tb)

        # Ensure tasks have been inserted by getting future results.
        while self.task_futures_info:
            self.task_futures_info = _future_tasks_get_result(
                self.task_futures_info)

        # Mark all tasks inserted.
        self._tasks_inserted = True

        return False
