const cliParams = require('./parsingParams');
const fetchingData = require('./fetchingData');
const build = require('./buildingChart');

fetchingData.setParams(cliParams);

let data = fetchingData.getData();

build(data);


