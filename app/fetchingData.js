const sqlite3 = require('sqlite3').verbose();
const query = require('./sqlQueries');

let maxValue = 0;
let queryResult = [];
const dataResult = [];

let getData = function (cliParams) {
  let db = new sqlite3.Database('./databases/olympic_history.db', (err) => {
    if (err) throw err;
  });

  return new Promise((resolve, reject) => {
    if (cliParams.chartName === 'medals') {
      let sql = getMedalsSql(cliParams);

      db.serialize(() => {
        db.each(sql.sqlQuery, sql.sqlParams, (err, row) => {
          if (err) throw err;

          if (row.medals > maxValue) {
            maxValue = row.medals;
          }
          queryResult.push(row);
        });
      });

      db.close((err) => {
        if (err) reject(err);

        for (let one of queryResult) {
          dataResult.push([one.year, normalization(one.medals)]);
        }

        resolve(dataResult);
      });
    } else if (cliParams.chartName === 'top-teams') {
      let sql = getTopTeamsSql(cliParams);

      db.serialize(() => {
        db.all(sql.sqlQuery, sql.sqlParams, (err, rows) => {
          if (err) throw err;

          maxValue = rows[0].medals;
          queryResult = rows;
        });
      });

      db.close((err) => {
        if (err) reject(err);

        for (let one of queryResult) {
          dataResult.push([one.noc_name, normalization(one.medals)]);
        }
        resolve(dataResult);
      });
    } else {
      db.close((err) => {
        if (err) reject(err);

        reject(new Error('Invalid chart name'));
      });
    }
  });
};

function getMedalsSql (cliParams) {
  let sqlQuery;
  let sqlParams;
  if (cliParams.medal) {
    sqlQuery = query.medalsMedal;
    sqlParams = [
      getSeasonName(cliParams.season),
      getMedalTitle(cliParams.medal),
      getSeasonName(cliParams.season),
      cliParams.noc_name
    ];
  } else {
    sqlQuery = query.medals;
    sqlParams = [
      getSeasonName(cliParams.season),
      getSeasonName(cliParams.season),
      cliParams.noc_name
    ];
  }
  return { sqlQuery, sqlParams };
}

function getTopTeamsSql (cliParams) {
  let sqlQuery;
  let sqlParams;
  if (cliParams.medal && cliParams.year) {
    sqlQuery = query.topTeamsMedalYear;
    sqlParams = {
      $medal: getMedalTitle(cliParams.medal),
      $season: getSeasonName(cliParams.season),
      $year: cliParams.year
    };
  } else if (cliParams.medal && !cliParams.year) {
    sqlQuery = query.topTeamsMedal;
    sqlParams = {
      $medal: getMedalTitle(cliParams.medal),
      $season: getSeasonName(cliParams.season)
    };
  } else if (!cliParams.medal && cliParams.year) {
    sqlQuery = query.topTeamsYear;
    sqlParams = {
      $season: getSeasonName(cliParams.season),
      $year: cliParams.year
    };
  } else {
    sqlQuery = query.topTeams;
    sqlParams = {
      $season: getSeasonName(cliParams.season)
    };
  }
  return { sqlQuery, sqlParams };
}

function normalization (arg) {
  return Math.round(arg * 200 / maxValue);
}

function getMedalTitle (arg) {
  let medalTitle;
  switch (arg) {
    case 'gold':
      medalTitle = 1;
      break;
    case 'silver':
      medalTitle = 2;
      break;
    case 'bronze':
      medalTitle = 3;
      break;
  }
  return medalTitle;
}

function getSeasonName (arg) {
  let season;
  switch (arg) {
    case 'winter':
      season = 1;
      break;
    case 'summer':
      season = 0;
      break;
  }
  return season;
}

module.exports = getData;
