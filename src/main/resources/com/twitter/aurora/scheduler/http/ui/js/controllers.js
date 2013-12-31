'use strict';

/* Controllers */

angular.module('auroraUI.controllers', []).
    controller('AuroraUI.JobSummaryController',
    function ($scope, $window, auroraClient) {
      $scope.title = 'Scheduled Jobs Summary';

      $scope.columnCollection = [
        {label : 'Role', map: 'role', cellTemplateUrl: 'roleLink.html'},
        {label : 'Jobs', map: 'jobCount'},
        {label : 'Cron Jobs', map: 'cronJobCount'}
      ];

      $scope.rowCollection = auroraClient.getJobSummary().summaries;

      $scope.globalConfig = {
        isGlobalSearchActivated: true,
        isPaginationEnabled: true,
        itemsByPage: 25,
        maxSize: 8,
        selectionMode: 'single'
      };

      $scope.$on('selectionChange', function (event, args) {
        $window.location.href = args.item.role;
      });
   });
