'use strict';

auroraUI.directive('roleLink', function () {
  return {
    restrict: 'C',
    template: "<a ng-href='/{{formatedValue}}'>{{formatedValue}}</a>"
  };
});