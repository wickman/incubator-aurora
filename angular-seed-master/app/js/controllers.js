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

      $scope.jobsummaries = $scope.jobSummary.jobSummaries;

      $scope.columnCollection = [
        {label : 'role', map: 'role'},
        {label : 'jobs', map: 'jobs'},
        {label : 'cronJobs', map: 'cronJobs'}
      ];

      $scope.rowCollection = $scope.jobSummary.jobSummaries;

      $scope.globalConfig = {
        isGlobalSearchActivated: true
      };

      $scope.filterOptions = {
        filterText: "",
        useExternalFilter: false
      };

      $scope.gridOptions = {
        data: 'jobsummaries',
        enablePaging: true,
        pagingOptions: {
          pageSizes: [10, 25, 50, 1000],
          pageSize: 1,
          currentPage: 1
        },
        showFooter: true,
        enableCellSelection: false,
        enableRowSelection: false,
        filterOptions: 'filterOptions',
        showFilter: true
      };
    });
