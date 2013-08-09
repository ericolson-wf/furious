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

import unittest

from mock import patch
from mock import Mock

from furious.context.task_futures import insert_tasks_async
from furious.context.task_futures import future_tasks_get_result
from furious.context.task_futures import FutureInfo


class TestInsertTasksAsync(unittest.TestCase):
    """Test insert_tasks_async()."""

    @patch('google.appengine.api.taskqueue.Queue.add_async', auto_spec=True)
    def test_no_tasks(self, add_async):
        """When no tasks are passed in, ensure insert_tasks_async() does not
        raise exceptions and return None.
        """

        tasks = []
        queue, transactional = Mock(), Mock()
        return_value = insert_tasks_async(
            tasks, queue, transactional=transactional)

        # Since we had no tasks, ensure add_async() was not called.
        add_async.assert_calleds_once_with(tasks, transactional=transactional)

        # Ensure None was returned
        self.assertEqual(None, return_value)

    @patch('google.appengine.api.taskqueue.Queue.add_async', auto_spec=True)
    def test_with_tasks(self, add_async):
        """When tasks are passed to, insert_tasks_async() does not raise
        exceptions and return None.
        """

        tasks = ["task1", "task2"]
        queue = "queue-name"
        transactional = Mock()
        future_info = insert_tasks_async(
            tasks, queue, transactional=transactional)

        # Ensure add_async() was called with the correct arguments.
        add_async.assert_calleds_once_with(tasks, transactional=transactional)

        # Ensure the resulting future was returned in a FutureInfo struct.
        future = add_async.return_value
        self.assertEqual(future_info, (future, tasks, queue, transactional))

    @patch('google.appengine.api.taskqueue.Queue.add_async', auto_spec=True)
    def test_with_tasks(self, add_async):

        tasks = ["task1", "task2"]
        queue = "queue-name"
        transactional = Mock()
        future_info = insert_tasks_async(
            tasks, queue, transactional=transactional)

        # Ensure add_async() was called with the correct arguments.
        add_async.assert_calleds_once_with(tasks, transactional=transactional)

        # Ensure the resulting future was returned in a FutureInfo struct.
        future = add_async.return_value
        self.assertEqual(future_info, (future, tasks, queue, transactional))


class TestFutureTasksGetResult(unittest.TestCase):
    """Test future_tasks_get_result()."""

    @patch('furious.context.task_futures.insert_tasks_async', auto_spec=True)
    def test_no_futures(self, insert_tasks_async):
        """When we don't pass any futures, just make sure no exception is
        raised and an empty list is returned.
        """

        futures = []

        reinserted_futures = future_tasks_get_result(futures)

        # Since we had no futures, ensure insert_tasks_async() was not called.
        self.assertEqual(0, insert_tasks_async.call_count)

        # Ensure None was returned
        self.assertEqual([], reinserted_futures)

    @patch('furious.context.task_futures.insert_tasks_async', auto_spec=True)
    def test_with_futures(self, insert_tasks_async):
        """When we pass in futures to future_tasks_get_result(), ensure that
        get_result() was called on each future.
        Also, since the future's get_result() doesn't raise exceptions, ensure
        an empty list is returned.
        """

        transactional = False
        future_info1 = FutureInfo(
            Mock(), ["task1"], "queuename", transactional)
        future_info2 = FutureInfo(
            Mock(), ["task2", "task3"], "queuename", transactional)
        futures = [future_info1, future_info2]

        reinserted_futures = future_tasks_get_result(futures)

        # Ensure get_result() was called on the futures.
        future_info1[0].get_result.assert_called_once_with()
        future_info2[0].get_result.assert_called_once_with()

        # Since we had no futures, ensure insert_tasks_async() was not called.
        self.assertEqual(0, insert_tasks_async.call_count)

        # Ensure None was returned
        self.assertEqual([], reinserted_futures)

    @patch('furious.context.task_futures.insert_tasks_async', auto_spec=True)
    def test_with_future_raising_exception(self, insert_tasks_async):
        """When future.get_result raises an Exception, ensure that
        insert_tasks_async() is called, once for each half of the tasks.
        """

        from google.appengine.api import taskqueue

        transactional = False
        future = Mock()
        future.get_result.side_effect = taskqueue.BadTaskStateError

        future_info1 = FutureInfo(
            future, ["task1", "task2", "task3", "task4"], "queuename",
            transactional)
        futures = [future_info1]

        # Setup the return values for insert_tasks_async()
        insert_tasks_async.side_effect = ['new_future1', 'new_future2']

        reinserted_futures = future_tasks_get_result(futures)

        expected_reinsert1 = FutureInfo(
            'new_future1', ['task1', 'task2'], "queuename", transactional)
        self.assertEqual(reinserted_futures[0], expected_reinsert1)

        expected_reinsert2 = FutureInfo(
            'new_future2', ['task3', 'task4'], "queuename", transactional)
        self.assertEqual(reinserted_futures[1], expected_reinsert2)
