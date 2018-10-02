const medalsMedal = `SELECT t1.year, t2.medals
                     FROM (SELECT year 
                          FROM games 
                          WHERE season=?) t1
                     LEFT JOIN (SELECT year, noc_name, COUNT(medal) AS medals
                                FROM athletes,results,games,teams
                                WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                                  AND medal=? AND season=? AND noc_name=?
                                GROUP BY noc_name, year) t2
                     ON t1.year = t2.year
                     ORDER BY t1.year ASC;`;

const medals = `SELECT t1.year, t2.medals
                FROM (SELECT year 
                      FROM games 
                      WHERE season=?) t1
                LEFT JOIN (SELECT year, noc_name, COUNT(medal) AS medals
                           FROM athletes,results,games,teams
                           WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                             AND medal in (1,2,3) AND season=? AND noc_name=?
                           GROUP BY noc_name, year) t2
                ON t1.year = t2.year
                ORDER BY t1.year ASC;`;

const topTeamsMedalYear = `SELECT noc_name, count(medal) AS medals
                           FROM athletes,results,games,teams
                           WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                              AND medal=$medal AND season=$season AND year=$year
                           GROUP BY noc_name, year
                           HAVING medals >= (SELECT AVG(medals)
                                             FROM (SELECT noc_name, COUNT(medal) AS medals
                                                   FROM athletes,results,games,teams
                                                   WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                                                      AND medal=$medal AND season=$season AND year=$year
                                                   GROUP BY noc_name, year)
                                             )
                           ORDER BY medals DESC;`;

const topTeamsMedal = `SELECT noc_name, COUNT(medal) AS medals
                       FROM athletes,results,games,teams
                       WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                          AND medal=$medal AND season=$season
                       GROUP BY noc_name
                       HAVING medals >= (SELECT AVG(medals)
                                         FROM (SELECT noc_name, COUNT(medal) AS medals
                                               FROM athletes,results,games,teams
                                               WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                                                  AND medal=$medal AND season=$season
                                               GROUP BY noc_name)
                                         )
                       ORDER BY medals DESC;`;

const topTeamsYear = `SELECT noc_name, COUNT(medal) AS medals
                      FROM athletes,results,games,teams
                      WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                            AND medal in (1,2,3) AND season=$season AND year=$year
                      GROUP BY noc_name, year
                      HAVING medals >= (SELECT AVG(medals)
                                        FROM (select noc_name, COUNT(medal) AS medals
                                              FROM athletes,results,games,teams
                                              WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                                                AND medal in (1,2,3) AND season=$season AND year=$year
                                              GROUP BY noc_name, year)
                                        )
                      ORDER BY medals DESC;`;

const topTeams = `SELECT noc_name, COUNT(medal) AS medals
                  FROM athletes,results,games,teams
                  WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                    AND medal in (1,2,3) AND season=$season
                  GROUP BY noc_name
                  HAVING medals >= (SELECT AVG(medals)
                                    FROM (SELECT noc_name, COUNT(medal) AS medals
                                          FROM athletes,results,games,teams
                                          WHERE athlete_id=athletes.id AND game_id=games.id AND team_id=teams.id
                                             AND medal in (1,2,3) AND season=$season
                                          GROUP BY noc_name)
                                    )
                  ORDER BY medals DESC;`;

module.exports = {
  medalsMedal,
  medals,
  topTeamsMedalYear,
  topTeamsMedal,
  topTeamsYear,
  topTeams
};
