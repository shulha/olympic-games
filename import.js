const sqlite3 = require('sqlite3').verbose();
const readline = require('readline');
const fs = require('fs');

const rl = readline.createInterface({
  input: fs.createReadStream('databases/sandbox.csv', {
    start: 80
  }),
  crlfDelay: Infinity
});

let sportsSet = new Set();
let eventsSet = new Set();
let athletesObj = {};
let teamsObj = {};
let gamesObj = {};
let resultsArray = [];

let prevGame = '';
let prevCity = '';

let teamsTable = [];

rl.on('line', (line) => {
  let arr = line.split(',');

  let sex = (arr[2] !== 'NA') ? arr[2] : null;
  let age = +arr[3] ? +arr[3] : null;
  let params = {};
  if (+arr[4]) params['height'] = +arr[4];
  if (+arr[5]) params['weight'] = +arr[5];

  let noc = arr[7];

  // let athleteName = arr[1].replace(/"+([^"]+)"+\s|"|(\(.*?\))*/g, '').trim();
  let athleteName = arr[1];

  let athleteData = [
    sex,
    age,
    JSON.stringify(params),
    noc
  ];
  athletesObj[athleteName] = athleteData;

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
});

rl.on('close', function () {
  let db = new sqlite3.Database('databases/olympic_history.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
      return console.error(err.message);
    }
    console.log('Connected to the olympic_history database.');
  });

  db.parallelize(function () {
    insertSet(sportsSet, db, 'sports');

    insertSet(eventsSet, db, 'events');

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
            return console.error(err.message);
          }
          console.log(`Rows inserted ${this.changes} to games`);
        }
      );
    }
  });

  db.serialize(() => {
    for (let key in teamsObj) {
      db.run(
        'INSERT INTO teams (name, noc_name) VALUES (?,?)',
        teamsObj[key], key,
        function (err) {
          if (err) {
            return console.error(err.message);
          }
          console.log(`Rows inserted ${this.changes} to teams`);
          teamsTable[key] = this.lastID;
        }
      );
    }
    db.serialize(() => {
      db.all('SELECT id, noc_name FROM teams', [], (err, rows) => {
        if (err) {
          throw err;
        }
        rows.forEach((row) => {
          teamsTable[row.noc_name] = row.id;
          console.log(row.noc_name);
          console.log(teamsTable);
        });
      });
      for (let athlete in athletesObj) {
        console.log(teamsTable);
        db.run(
          'INSERT INTO athletes (full_name, age, sex, params, team_id) VALUES (?,?,?,?,?)',
          // athlete, athletesObj[athlete][0], athletesObj[athlete][1], athletesObj[athlete][2], teamsTable[athletesObj[athlete][3]],
          athlete, athletesObj[athlete][0], athletesObj[athlete][1], athletesObj[athlete][2], athletesObj[athlete][3],
          function (err) {
            if (err) {
              return console.error(err.message);
            }
            console.log(`Rows inserted ${this.changes} to athletes`);
            console.log(teamsTable);
          }
        );
      }
    });
  });

  db.close((err) => {
    if (err) {
      return console.error(err.message);
    }
    console.log('Close the database connection.');
  });
});

function insertSet (set, db, table) {
  let array = Array.from(set);
  let placeholders = array.map((arg) => '(?)').join(',');
  let sql = 'INSERT INTO ' + table + '(name) VALUES ' + placeholders;
  db.run(sql, array, function (err) {
    if (err) {
      return console.error(err.message);
    }
    console.log(`Rows inserted ${this.changes} to ${table}`);
  });
}
