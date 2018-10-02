const cliParams = require('./app/parsingParams');
const fetchingData = require('./app/fetchingData');

(async () => {
  try {
    let result = await fetchingData(cliParams);
    result.forEach(function (row) {
      console.log(row[0] + ' ' + '█'.repeat(Number(row[1])));
    });
  } catch (err) {
    console.log(err);
  }
})();
