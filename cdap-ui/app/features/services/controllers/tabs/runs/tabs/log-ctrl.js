angular.module(PKG.name + '.feature.services')
  .controller('ServicesRunDetailLogController', function($scope, $state, myServiceApi) {

    $scope.logs = [];

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      serviceId: $state.params.programId,
      runId: $scope.runs.selected.runid,
      max: 50,
      scope: $scope
    };

    myServiceApi.logs(params)
      .$promise
      .then(function (res) {
        $scope.logs = res;
      });

  });
