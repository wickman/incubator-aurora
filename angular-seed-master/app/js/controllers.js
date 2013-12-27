'use strict';

/* Controllers */

angular.module('auroraUI.controllers', []).
    controller('AuroraUI.JobSummaryController',
    function ($scope) {
      $scope.title = 'Scheduled Jobs Summary';
      $scope.jobSummary = { 'jobSummaries': [
        {
          'role': 'mesos',
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
        {label : 'role', map: 'role'},
        {label : 'jobs', map: 'jobs'},
        {label : 'cronJobs', map: 'cronJobs'}
      ];
      $scope.rowCollection = $scope.jobSummary.jobSummaries;
    });
