
from .context import Context
from .context import _task_batcher
from .task_futures import _future_tasks_get_result
from .task_futures import _insert_tasks_async

from .. import errors


class AutoContext(Context):
    """Similar to context, but automatically inserts tasks asynchronously as
    they are added to the context.  Inserted in batches if specified.
    """

    def __init__(self, batch_size=None, **options):
        """Setup this context in addition to accepting a batch_size."""

        Context.__init__(self, **options)

        self.batch_size = batch_size

        self._num_tasks_inserted = 0

        self._insert_tasks_async = options.pop(
            'insert_tasks_async', _insert_tasks_async)
        if not callable(self._insert_tasks_async):
            raise TypeError(
                'You must provide a valid insert_tasks_async function.')

        # Futures representing async tasks.
        self.task_futures_info = []

    def add(self, target, args=None, kwargs=None, **options):
        """Add an Async job to this context.

        The same as Context.add, but calls _auto_insert_check() to
        insert tasks automatically.
        """

        target = super(
            AutoContext, self).add(target, args, kwargs, **options)

        self._auto_insert_check()

        return target

    def _auto_insert_check(self):
        """Automatically insert tasks asynchronously.
        Depending on batch_size, insert or wait until next call.
        """

        if not self.batch_size:
            self._handle_tasks()
            return

        if len(self.tasks) >= self.batch_size:
            self._handle_tasks()

    def _handle_tasks_insert(self, batch_size=None):
        """Convert all Async's into tasks, then insert them into queues."""
        if self._tasks_inserted:
            raise errors.ContextAlreadyStartedError(
                "This Context has already had its tasks inserted.")

        task_map = self._get_tasks_by_queue()
        for queue, tasks in task_map.iteritems():
            for batch in _task_batcher(tasks, batch_size=batch_size):
                self.task_futures_info.append(
                    self._insert_tasks_async(batch, queue=queue))

    def _handle_tasks(self):
        """Convert Async's into tasks, then insert them into queues.
        Similar to the default _handle_tasks, but don't mark all
        tasks inserted.
        """

        self._handle_tasks_insert(batch_size=self.batch_size)

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
