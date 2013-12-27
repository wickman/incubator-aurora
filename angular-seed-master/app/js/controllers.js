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

      // TODO: ng-Grid improvements:
      // Fix the search box location in the grid.
      // Fix the pagination display style..
      // Show all in page size display.
      // header instead of footer.
      // Order of pagination.
    });
