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

from collections import namedtuple

# Structure to help store and reinsert task futures.
FutureInfo = namedtuple('FutureInfo', "future tasks queue transactional")


def insert_tasks_async(tasks, queue, transactional=False):
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


def future_tasks_get_result(futures):

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
                # Adding this single task failed, so don't try adding it again.
                return

            queue = future_info.queue
            transactional = future_info.transactional

            # Reinsert half
            half_tasks_1 = tasks[:count / 2]
            new_future_1 = insert_tasks_async(
                half_tasks_1, queue, transactional)
            reinserted_futures.append(
                FutureInfo(new_future_1, half_tasks_1, queue, transactional))

            # Reinsert other half
            half_tasks_2 = tasks[count / 2:]
            new_future_2 = insert_tasks_async(
                half_tasks_2, queue, transactional)
            reinserted_futures.append(
                FutureInfo(new_future_2, half_tasks_2, queue, transactional))

    return reinserted_futures
