module.exports = function (data) {
  for (let row of data) {
    console.log(row[0] + ' ' + '█'.repeat(Number(row[1])));
  }
};
