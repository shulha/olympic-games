const cliParams = require('./app/parsingParams');
const fetchingData = require('./app/fetchingData');

(async () => {
  try {
    let result = await fetchingData(cliParams);
    ((data) => {
      for (let row of data) {
        console.log(row[0] + ' ' + '█'.repeat(Number(row[1])));
      }
    })(result);
  } catch (err) {
    console.log(err);
  }
})();
