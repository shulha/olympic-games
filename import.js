const sqlite3 = require('sqlite3').verbose();
const readline = require('readline');
const fs = require('fs');
const col = require('./databases/columnName');

const rl = readline.createInterface({
  input: fs.createReadStream('databases/sandbox.csv', {
    start: 80,
  }),
  crlfDelay: Infinity,
});

const sportsSet = new Set();
const eventsSet = new Set();
const athletesObj = {};
const teamsObj = {};
const gamesObj = {};
const resultsArray = [];

const medals = {
  NA: 0,
  Gold: 1,
  Silver: 2,
  Bronze: 3,
};

// функция конвертирует CSV-строку в массив, разделяя ее на элементы массива по запятым,
// но если запятая встречается в слове, которое в кавычках,
// то такое слово переносится в массив целиком не разделяясь
function csvToArray(str) {
  const row = [''];
  let prevChar = ''; let i = 0; let quote = true;

  for (let c = 0; c < str.length; c += 1) {
    if (str[c] === '"') {
      if (quote && str[c] === prevChar) row[i] += str[c];
      quote = !quote;
    } else if (str[c] === ',' && quote) {
      i += 1;
      str[c] = row[i] = '';
    } else {
      row[i] += str[c];
    }
    prevChar = str[c];
  }
  return row;
}

let prevGame = '';
let prevCity = '';

rl.on('line', (line) => {
  const arr = csvToArray(line);

  const sex = (arr[col.sex] !== 'NA') ? arr[col.sex] : null;
  const age = +arr[col.age] ? +arr[col.age] : null;
  const params = {};
  if (arr[col.height]) params.height = +arr[col.height];
  if (arr[col.weight]) params.weight = +arr[col.weight];

  const noc = arr[col.NOC];

  const athleteName = arr[col.name].replace(/"+([^"]+)"+\s|"|(\(.*?\))*/g, '').trim();

  athletesObj[athleteName] = [
    sex,
    age,
    JSON.stringify(params),
    noc,
  ];

  sportsSet.add(arr[col.sport]);

  eventsSet.add(arr[col.evnt]);

  teamsObj[noc] = arr[col.team].replace(/-\d+$/g, '');

  const nextGame = arr[col.games];
  const nextCity = arr[col.city];
  if (nextGame === '1906 Summer') {} else if (prevGame === nextGame && prevCity !== nextCity) {
    gamesObj[nextGame] += `, ${nextCity}`;
  } else {
    gamesObj[nextGame] = nextCity;
  }
  prevCity = nextCity;
  prevGame = nextGame;

  if (nextGame !== '1906 Summer') {
    resultsArray.push([
      athleteName,
      arr[col.games],
      arr[col.sport],
      arr[col.evnt],
      medals[arr[col.medal]],
    ]);
  }
});

rl.on('close', () => {
  const db = new sqlite3.Database('databases/test_db.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) throw err;

    console.log('Connected to the olympic_history database.');
  });

  let resultsCnt = 0; let gamesCnt = 0; let teamsCnt = 0; let athletesCnt = 0;
  db.serialize(() => {
    insertSet(sportsSet, db, 'sports');

    insertSet(eventsSet, db, 'events');

    db.serialize(() => {
      db.run('begin');
      for (const oneRow of resultsArray) {
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
          },
        );
      }
      for (const game in gamesObj) {
        const cities = gamesObj[game];
        const gameArray = game.split(' ');
        const year = gameArray[0];
        const season = (gameArray[1] === 'Summer') ? 0 : 1;
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
          },
        );
      }
      for (const key in teamsObj) {
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
          },
        );
      }

      for (const athlete in athletesObj) {
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
          },
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

function updateTable(db, colName, selectedTable) {
  const tmpArray = [];
  db.all(`SELECT id, ${colName} FROM ${selectedTable}s`, [], (err, rows) => {
    if (err) throw err;

    if (selectedTable === 'game') {
      rows.forEach((row) => {
        const season = (row.season === 0) ? 'Summer' : 'Winter';
        tmpArray.push([row.id, `${row.year} ${season}`]);
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

    for (const data of tmpArray) {
      const sql = `UPDATE ${_table}
        SET ${selectedTable}_id = ?
        WHERE ${selectedTable}_id = ?`;

      db.run(sql, data, function (err) {
        if (err) throw err;

        console.log(`Row(s) updated: ${this.changes} in "${_table}"`);
      });
    }
  });
}

function insertSet(set, db, table) {
  const array = Array.from(set);
  const placeholders = array.map(() => '(?)').join(',');
  const sql = `INSERT INTO ${table}(name) VALUES ${placeholders}`;
  db.run(sql, array, function (err) {
    if (err) throw err;

    console.log(`Rows inserted ${this.changes} to "${table}"`);
  });
}
