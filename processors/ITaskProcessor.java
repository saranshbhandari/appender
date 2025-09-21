
package com.test.dataflowengine.processors;

import com.test.dataflowengine.models.ETLTask;
import com.test.dataflowengine.models.JobDetails;
import com.test.dataflowengine.models.TaskStatus;

public interface ITaskProcessor {
    TaskStatus ProcessTask(ETLTask task, JobDetails job);
}
