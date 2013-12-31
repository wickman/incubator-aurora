'use strict';

auroraUI.factory(
    'auroraClient', function () {
      return {
        jobSummary: { 'jobSummaries': [
          {
            'role': "mesos",
            'jobs': 10,
            'cronJobs': 10
          },
          {
            'role': 'ads',
            'jobs': 101,
            'cronJobs': 20
          }
        ]}
      };
    });


