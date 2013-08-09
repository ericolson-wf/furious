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


class TestInsertTasksAsync(unittest.TestCase):
    """Test _insert_tasks_async()."""

    @patch('google.appengine.api.taskqueue.Queue.add_async', auto_spec=True)
    def test_no_tasks(self, add_async):

        tasks = []
        queue, transactional = Mock(), Mock()
        return_value = insert_tasks_async(
            tasks, queue, transactional=transactional)

        # Since we had not tasks, ensure add_async() was not called.
        add_async.assert_calleds_once_with(tasks, transactional=transactional)

        # Ensure None was returned
        self.assertEqual(None, return_value)

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
