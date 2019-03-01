package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"strings"
)

type Game struct {
	event string
	site string
	eventdate string
	date string
	round string
	white string
	black string
	result string
	whiteelo string
	blackelo string
	eco string
	pgn string
}

const (
	tag_event = "[Event "
	tag_site = "[Site "
	tag_date = "[Date "
	tag_event_date = "[EventDate "
	tag_round = "[Round "
	tag_white = "[White "
	tag_black = "[Black "
	tag_result = "[Result "
	tag_white_elo = "[WhiteElo "
	tag_black_elo = "[BlackElo "
	tag_eco = "[ECO "
)

const (
	user     = "root"
	password = ""
)

func main() {
	pgn_path := "./PGNs"
	files, err := ioutil.ReadDir(pgn_path)
	check(err)

	db, err := sql.Open("mysql", user + ":" + password + "@/pgns")
	check(err)

	for _, file := range files {
		dat, err := ioutil.ReadFile(pgn_path + "/" + file.Name())
		check(err)

		// because this tag is used to separate games,
		// it's set apart from the rest of the kv pairs
		games := strings.Split(string(dat), tag_event)

		for _, game := range games {
			this_game := Game{}

			game_lines := strings.Split(game, "\n")
			if (len(game_lines[0])) == 0 {
				continue
			}

			builder := &this_game
			replacer := strings.NewReplacer("]", "", "\"", "")

			value_pgn := ""
			for _, game_line := range game_lines {
				if (len(builder.event) == 0) {
					// event has not been set.
					event := replacer.Replace(game_line)
					builder.event = strings.TrimSpace(event)
					continue
				}

				if strings.Contains(game_line, tag_site) {
					builder.site = getValueFromString(game_line, tag_site)
					continue
				}

				if strings.Contains(game_line, tag_date) {
					builder.date = getValueFromString(game_line, tag_date)
					continue
				}

				if strings.Contains(game_line, tag_event_date) {
					builder.eventdate = getValueFromString(game_line, tag_event_date)
					continue
				}

				if strings.Contains(game_line, tag_round) {
					builder.round = getValueFromString(game_line, tag_round)
					continue
				}

				if (strings.Contains(game_line, tag_white)) {
					builder.white = getValueFromString(game_line, tag_white)
					continue
				}

				if strings.Contains(game_line, tag_black) {
					builder.black = getValueFromString(game_line, tag_black)
					continue
				}

				if strings.Contains(game_line, tag_result) {
					builder.result = getValueFromString(game_line, tag_result)
					continue
				}

				if strings.Contains(game_line, tag_white_elo) {
					builder.whiteelo = getValueFromString(game_line, tag_white_elo)
					continue
				}

				if strings.Contains(game_line, tag_black_elo) {
					builder.blackelo = getValueFromString(game_line, tag_black_elo)
					continue
				}


				if strings.Contains(game_line, tag_eco) {
					builder.eco = getValueFromString(game_line, tag_eco)
					continue
				}

				value_pgn += strings.TrimSpace(game_line) + " "
			}

			builder.pgn = strings.TrimSpace(value_pgn)

			file_name := file.Name()

			sql_insert := "INSERT INTO `pgns`.`Games` (`File`, `Event`, `Site`, `Date`, `Round`, `White`, `Black`, `Result`, `WhiteELO`, `BlackELO`, `ECO`, `PGN`) " +
				"VALUES " +
				"('" + file_name + "', " +
				"'" + builder.event + "', " +
				"'" + builder.site + "', " +
				"'" + builder.eventdate + "', " +
				"'" + builder.round + "', " +
				"'" + builder.white + "', " +
				"'" + builder.black + "', " +
				"'" + builder.result + "', " +
				"'" + builder.whiteelo + "', " +
				"'" + builder.blackelo + "', " +
				"'" + builder.eco + "', " +
				"'" + builder.pgn + "');"

			fmt.Println(sql_insert)

			// perform a db.Query insert
			insert, err := db.Query(sql_insert)
			check(err)

			defer func() {
				if err := insert.Close(); err != nil {
					check(err)
				}
			}()
		}
	}

	defer func() {
		if err := db.Close(); err != nil {
			check(err)
		}
	}()
}

func getValueFromString(line string, tag string) string {
	replacer := strings.NewReplacer("]", "", "\"", "")
	tag_replacer := strings.NewReplacer(tag, "")
	quote_replacer := strings.NewReplacer("'", "''")

	builder := replacer.Replace(line)
	builder = tag_replacer.Replace(builder)
	builder = strings.TrimSpace(builder)

	return quote_replacer.Replace(builder)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}