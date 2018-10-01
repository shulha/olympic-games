const cliParams = require('./parsingParams');
const fetchingData = require('./fetchingData');
const build = require('./buildingChart');

(async () => {
  try {
    let result = await fetchingData(cliParams);
    build(result);
  } catch (err) {
    console.log(err);
  }
})();
