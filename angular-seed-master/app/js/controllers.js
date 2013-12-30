'use strict';

/* Controllers */

angular.module('auroraUI.controllers', []).
    controller('AuroraUI.JobSummaryController',
    function ($scope) {
      $scope.title = 'Scheduled Jobs Summary';

      $scope.jobSummary = { 'jobSummaries': [
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
      ]};

      $scope.columnCollection = [
        {label : 'Role', map: 'role', cellTemplateUrl: 'roleLink.html'},
        {label : 'Jobs', map: 'jobs'},
        {label : 'Cron Jobs', map: 'cronJobs'}
      ];

      $scope.rowCollection = $scope.jobSummary.jobSummaries;

      $scope.globalConfig = {
        isGlobalSearchActivated: true,
        isPaginationEnabled: true,
        itemsByPage: 25,
        maxSize: 8,
        selectionMode: 'single'
      };

      $scope.$on('selectionChange', function (event, args) {
        console.log(args);
      });
    });
