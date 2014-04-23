'use strict';

auroraUI.factory(
  'auroraClient',
  ['$window',
    function ($window) {
      var auroraClient = {
        // Each of the functions below wrap an API call on the scheduler.
        getRoleSummary: function () {
          var response = auroraClient.getSchedulerClient().getRoleSummary();
          var result = auroraClient.processResponse(response);
          result.summaries = response.result !== null
            ? response.result.roleSummaryResult.summaries : [];
          return result;
        },

        getJobSummary: function (role) {
          var response = auroraClient.getSchedulerClient().getJobSummary(role);
          var result = auroraClient.processResponse(response);
          result.jobs = response.result !== null ? response.result.jobSummaryResult.summaries : [];
          return result;
        },

        getQuota: function (role) {
          var response = auroraClient.getSchedulerClient().getQuota(role);
          var result = auroraClient.processResponse(response);
          result.quota = response.result !== null ? response.result.getQuotaResult : [];
          return result;
        },

        getTasks: function (role, environment, jobName) {
          var id = new Identity();
          id.role = role;
          var taskQuery = new TaskQuery();
          taskQuery.identity = id;
          taskQuery.environment = environment;
          taskQuery.jobName = jobName;
          var response = auroraClient.getSchedulerClient().getTasksStatus(taskQuery);
          var result = auroraClient.processResponse(response);
          result.tasks = response.result !== null ? response.result.scheduleStatusResult.tasks : [];
          return result;
        },

        // Utility functions
        // TODO(Suman Karumuri): Make schedulerClient a service
        schedulerClient: null,

        getSchedulerClient: function () {
          if (!auroraClient.schedulerClient) {
            var transport = new Thrift.Transport("/api/");
            var protocol = new Thrift.Protocol(transport);
            auroraClient.schedulerClient = new ReadOnlySchedulerClient(protocol);
            return auroraClient.schedulerClient;
          } else {
            return auroraClient.schedulerClient;
          }
        },

        processResponse: function (response) {
          auroraClient.setPageTitle(response.serverInfo);
          return {
            error: response.responseCode !== 1
              ? (response.message || 'No error message returned by the scheduler')
              : '',
            statsUrlPrefix: response.serverInfo && response.serverInfo.statsUrlPrefix
              ? response.serverInfo.statsUrlPrefix
              : ''
          };
        },

        getPageTitle: function (info) {
          var title = "Aurora UI";
          return _.isNull(info) || info.error || typeof info.clusterName === "undefined"
            ? title
            : info.clusterName + " " + title;
        },

        setPageTitle: function (serverInfo) {
          $window.document.title = auroraClient.getPageTitle(serverInfo);
        }
      };
      return auroraClient;
    }
  ]);

auroraUI.factory(
  'taskUtil',
  function () {
    var taskUtil = {
      // Given a list of tasks, group tasks with identical task configs and belonging to contiguous
      // instance ids together.
      summarizeActiveTaskConfigs: function (tasks) {
        return _.chain(tasks)
          .filter(taskUtil.isActiveTask)
          .map(function (task) {
            return {
              instanceId: task.assignedTask.instanceId,
              schedulingDetail: taskUtil.configToDetails(task.assignedTask.task)
            };
          })
          .groupBy(function (task) {
            return JSON.stringify(task.schedulingDetail);
          })
          .map(function (tasks) {
            // Given a list of tasks with the same task config, group the tasks into ranges where
            // each range consists of consecutive task ids along with their task config.
            var schedulingDetail = _.first(tasks).schedulingDetail;
            var ranges = taskUtil.toRanges(_.pluck(tasks, 'instanceId'));
            return _.map(ranges, function (range) {
              return {
                range: range,
                schedulingDetail: schedulingDetail
              }
            });
          })
          .flatten(true)
          .sortBy(function (scheduleDetail) {
            return scheduleDetail.range.start;
          })
          .value();
      },

      configToDetails: function (task) {
        var constraints = _.chain(task.constraints)
          .sortBy(function (constraint) {
            return constraint.name;
          })
          .map(taskUtil.formatConstraint)
          .value()
          .join(', ');

        var metadata = _.chain(task.metadata)
          .sortBy(function (metadata) {
            return metadata.key;
          })
          .map(function (metadata) {
            return metadata.key + ':' + metadata.value;
          })
          .value()
          .join(', ');

        return {
          numCpus: task.numCpus,
          ramMb: task.ramMb,
          diskMb: task.diskMb,
          isService: task.isService,
          production: task.production,
          contact: task.contactEmail || '',
          ports: _.sortBy(task.requestedPorts).join(', '),
          constraints: constraints,
          metadata: metadata
        };
      },

      // Given a list of instanceIds, group them into contiguous ranges.
      toRanges: function (instanceIds) {
        instanceIds = _.sortBy(instanceIds);
        var ranges = [];
        var i = 0;
        var start = instanceIds[i];
        var end = instanceIds[i];
        while (i < instanceIds.length) {
          if ((i + 1 === instanceIds.length) || (instanceIds[i] + 1 !== instanceIds[i + 1])) {
            ranges.push({start: start, end: instanceIds[i]});
            i++;
            start = instanceIds[i];
          } else {
            i++;
          }
        }
        return ranges;
      },

      // A function that converts a task constraint into a string
      formatConstraint: function (constraint) {
        var taskConstraint = constraint.constraint;

        var valueConstraintStr = '';
        var valueConstraint = taskConstraint.value;
        if (valueConstraint && valueConstraint.values && _.isArray(valueConstraint.values)) {
          var values = valueConstraint.values.join(',');
          valueConstraintStr = valueConstraint.negated ? "not " + values : values;
        }

        var limitConstraintStr = taskConstraint.limit ? JSON.stringify(taskConstraint.limit) : '';

        if (_.isEmpty(limitConstraintStr) && _.isEmpty(valueConstraintStr)) {
          return '';
        } else {
          return constraint.name + ':' +
            (_.isEmpty(limitConstraintStr) ? valueConstraintStr : limitConstraintStr);
        }
      },

      isActiveTask: function (task) {
        return _.contains(ACTIVE_STATES, task.status);
      }
    };
    return taskUtil;
  });
