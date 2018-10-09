const cliParams = {};

const chartNames = ['medals', 'top-teams'];

const args = process.argv;
const chartName = args[2];
const chartParams = args.slice(3).map(
  param => param.toLowerCase(),
);

if (!chartNames.includes(chartName)) {
  console.warn('Error! Invalid chart name.');
  process.exit(1);
}

if (!chartParams.includes('winter') && !chartParams.includes('summer')) {
  console.warn('Error! You have to specify season.');
  process.exit(1);
}

cliParams.chartName = chartName;

if (chartName === 'medals') {
  let nocFlag = false;

  chartParams.forEach((param) => {
    if (param.length === 3) { nocFlag = true; }
  });

  if (!nocFlag) {
    console.log('NOC name is required!');
    process.exit(1);
  }
}

chartParams.forEach((param) => {
  if (Number.isInteger(Number(param))) {
    cliParams.year = +param;
  } else if (param === 'winter' || param === 'summer') {
    cliParams.season = param;
  } else if (param.length === 3) {
    cliParams.noc_name = param.toUpperCase();
  } else if (['gold', 'silver', 'bronze'].includes(param)) {
    cliParams.medal = param;
  } else {
    console.log(`Invalid param ${param}`);
    process.exit(1);
  }
});

module.exports = cliParams;
