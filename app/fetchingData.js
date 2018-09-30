let cliParams = null;

let setParams = function (params) {
  cliParams = params;
};

let getData = function () {
  console.log(cliParams);
  return [["USA", 50], ["RUS", 30], ["UGA", 10], ["UKR", 2]];
};

module.exports.setParams = setParams;
module.exports.getData = getData;
