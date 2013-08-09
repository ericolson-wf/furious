
from collections import namedtuple

# Structure to help store and reinsert task futures.
FutureInfo = namedtuple('FutureInfo', "future tasks queue transactional")


def _insert_tasks_async(tasks, queue, transactional=False):
    """Insert a batch of tasks into the specified queue. If an error occurs
    during insertion, split the batch and retry until they are successfully
    inserted.
    """
    from google.appengine.api import taskqueue

    if not tasks:
        return

    future = taskqueue.Queue(
        name=queue).add_async(tasks, transactional=transactional)

    return FutureInfo(future, tasks, queue, transactional)


def _future_tasks_get_result(futures):

    from google.appengine.api import taskqueue

    reinserted_futures = []

    for future_info in futures:

        future = future_info.future

        try:
            tasks = future.get_result()
        except (taskqueue.BadTaskStateError,
                taskqueue.TaskAlreadyExistsError,
                taskqueue.TombstonedTaskError,
                taskqueue.TransientError):

            tasks = future_info.tasks

            count = len(tasks)
            if count <= 1:
                return

            queue = future_info.queue
            transactional = future_info.transactional

            reinserted_futures.append(
                _insert_tasks_async(tasks[:count / 2], queue, transactional))
            reinserted_futures.append(
                _insert_tasks_async(tasks[count / 2:], queue, transactional))

    return reinserted_futures
