const sqlite3 = require('sqlite3').verbose();
const readline = require('readline');
const fs = require('fs');

const rl = readline.createInterface({
  input: fs.createReadStream('databases/athlete_events.csv', {
    start: 111
  }),
  crlfDelay: Infinity
});

const sportsSet = new Set();
const eventsSet = new Set();
const athletesObj = {};
const teamsObj = {};
const gamesObj = {};
const resultsArray = [];

const medals = {
  'NA': 0,
  'Gold': 1,
  'Silver': 2,
  'Bronze': 3
};

let prevGame = '';
let prevCity = '';

rl.on('line', (line) => {
  const arr = csvToArray(line).shift();

  const sex = (arr[2] !== 'NA') ? arr[2] : null;
  const age = +arr[3] ? +arr[3] : null;
  const params = {};
  if (arr[4]) params['height'] = +arr[4];
  if (arr[5]) params['weight'] = +arr[5];

  const noc = arr[7];

  const athleteName = arr[1].replace(/"+([^"]+)"+\s|"|(\(.*?\))*/g, '').trim();

  athletesObj[athleteName] = [
    sex,
    age,
    JSON.stringify(params),
    noc
  ];

  sportsSet.add(arr[12]);

  eventsSet.add(arr[13]);

  teamsObj[noc] = arr[6].replace(/-\d+$/g, '');

  let nextGame = arr[8];
  let nextCity = arr[11];
  if (nextGame === '1906 Summer') {} else if (prevGame === nextGame && prevCity !== nextCity) {
    gamesObj[nextGame] += ', ' + nextCity;
  } else {
    gamesObj[nextGame] = nextCity;
  }
  prevCity = nextCity;
  prevGame = nextGame;

  if (nextGame !== '1906 Summer') {
    resultsArray.push([athleteName, arr[8], arr[12], arr[13], medals[arr[14]]]);
  }
});

rl.on('close', function () {
  let db = new sqlite3.Database('databases/olympic_history.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) throw err;

    console.log('Connected to the olympic_history database.');
  });

  let resultsCnt = 0; let gamesCnt = 0; let teamsCnt = 0; let athletesCnt = 0;
  db.serialize(() => {
    insertSet(sportsSet, db, 'sports');

    insertSet(eventsSet, db, 'events');

    db.serialize(function () {
      db.run('begin');
      for (let oneRow of resultsArray) {
        db.run(
          'INSERT INTO results (athlete_id, game_id, sport_id, event_id, medal) VALUES (?,?,?,?,?)',
          oneRow,
          function (err) {
            if (err) {
              db.run('rollback');
              throw err;
            }

            resultsCnt += this.changes;
            console.log(`Rows inserted ${resultsCnt} to "results"`);
          }
        );
      }
      for (let game in gamesObj) {
        let cities = gamesObj[game];
        let gameArray = game.split(' ');
        let year = gameArray[0];
        let season = (gameArray[1] === 'Summer') ? 0 : 1;
        db.run(
          'INSERT INTO games (year, season, city) VALUES (?,?,?)',
          year, season, cities,
          function (err) {
            if (err) {
              db.run('rollback');
              throw err;
            }

            gamesCnt += this.changes;
            console.log(`Rows inserted ${gamesCnt} to "games"`);
          }
        );
      }
      for (let key in teamsObj) {
        db.run(
          'INSERT INTO teams (name, noc_name) VALUES (?,?)',
          teamsObj[key], key,
          function (err) {
            if (err) {
              db.run('rollback');
              throw err;
            }

            teamsCnt += this.changes;
            console.log(`Rows inserted ${teamsCnt} to "teams"`);
          }
        );
      }

      for (let athlete in athletesObj) {
        db.run(
          'INSERT INTO athletes (full_name, age, sex, params, team_id) VALUES (?,?,?,?,?)',
          athlete, athletesObj[athlete][0], athletesObj[athlete][1], athletesObj[athlete][2], athletesObj[athlete][3],
          function (err) {
            if (err) {
              db.run('rollback');
              throw err;
            }
            athletesCnt += this.changes;
            console.log(`Rows inserted ${athletesCnt} to "athletes"`);
          }
        );
      }
      db.run('commit');
    });
  });

  db.run('begin');
  try {
    updateTable(db, 'noc_name', 'team');
    updateTable(db, 'full_name', 'athlete');
    updateTable(db, 'name', 'sport');
    updateTable(db, 'name', 'event');
    updateTable(db, 'year, season', 'game');
  } catch (e) {
    db.run('rollback');
    throw e;
  }
  db.run('commit');

  db.close((err) => {
    if (err) throw err;

    console.log('Close the database connection.');
  });
});

function updateTable (db, colName, selectedTable) {
  let tmpArray = [];
  db.all('SELECT id, ' + colName + ' FROM ' + selectedTable + 's', [], (err, rows) => {
    if (err) throw err;

    if (selectedTable === 'game') {
      rows.forEach((row) => {
        let season = (row.season === 0) ? 'Summer' : 'Winter';
        tmpArray.push([row.id, row.year + ' ' + season]);
      });
    } else {
      rows.forEach((row) => {
        tmpArray.push([row.id, row[colName]]);
      });
    }

    let _table = 'results';
    if (selectedTable === 'team') {
      _table = 'athletes';
    }

    for (let data of tmpArray) {
      let sql = `UPDATE ${_table}
        SET ${selectedTable}_id = ?
        WHERE ${selectedTable}_id = ?`;

      db.run(sql, data, function (err) {
        if (err) throw err;

        console.log(`Row(s) updated: ${this.changes} in "${_table}"`);
      });
    }
  });
}

function insertSet (set, db, table) {
  let array = Array.from(set);
  let placeholders = array.map((arg) => '(?)').join(',');
  let sql = 'INSERT INTO ' + table + '(name) VALUES ' + placeholders;
  db.run(sql, array, function (err) {
    if (err) throw err;

    console.log(`Rows inserted ${this.changes} to "${table}"`);
  });
}

function csvToArray (text) {
  let p = ''; let row = ['']; let ret = [row]; let i = 0; let r = 0; let s = !0; let l;
  for (l of text) {
    if (l === '"') {
      if (s && l === p) row[i] += l;
      s = !s;
    } else if (l === ',' && s) l = row[++i] = '';
    else if (l === '\n' && s) {
      if (p === '\r') row[i] = row[i].slice(0, -1);
      row = ret[++r] = [l = '']; i = 0;
    } else row[i] += l;
    p = l;
  }
  return ret;
}
