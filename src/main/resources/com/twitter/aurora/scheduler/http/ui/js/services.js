'use strict';

auroraUI.factory(
  'auroraClient',
  function (auroraClientMock) {
    return {
      getJobSummary: function () {
        var client = this.makeSchedulerClient();

        var response;
        console.log("querying server");
        response = client.getJobSummary();
        console.log(response);
        return response.result.jobSummaryResult;
      },

      makeSchedulerClient: function () {
        var transport = new Thrift.Transport("/api/");
        var protocol = new Thrift.Protocol(transport);
        return new ReadOnlySchedulerClient(protocol);
      }
    };
  }
);

auroraUI.factory(
  'auroraClientMock',
  function () {
    return {
      getJobSummary: function () {
        var summary = new JobSummary();
        summary.role = "mesos";
        summary.jobCount = 10;
        summary.cronJobCount = 10;
        return { 'summaries': [summary]};
      }
    }
  }
);
