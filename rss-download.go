package main

import (
	"database/sql"
	"errors"
	"flag"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ungerik/go-rss"
)

// CREATE TABLE feeds (name TEXT PRIMARY KEY, url TEXT NOT NULL, dayOfWeek INTEGER NOT NULL,
// seconds INTEGER NOT NULL, lastTitle TEXT NOT NULL);

type updatedTitleMessage struct {
	Name  string
	Title string
}

// Flag specifications.
var dbFilename = flag.String("db_file", "feeds.db", "filename of database to use")
var target = flag.String("target", "", "target directory to download to")
var checkInterval = flag.Int(
	"check_interval", 3600, "seconds between checks during normal operation")
var rapidCheckInterval = flag.Int(
	"rapid_check_interval", 60, "seconds between checks when we suspect there will be a new item")
var rapidCheckDuration = flag.Int(
	"rapid_check_duration", 3600, "seconds that we suspect there will be a new item")
var downloadDelay = flag.Int(
	"download_delay", 30, "seconds to wait before downloading the file")
var requestDelay = flag.Int(
	"request_delay", 5, "seconds to wait between requests")
var checkImmediate = flag.Bool(
	"check_immediately", false, "if set, check immediately on startup")
var updateNotifyUrl = flag.String("update_notify_url", "", "url to push update notifications to")

var requestDelayTicker <-chan time.Time

func downloadUrl(url string) error {
	// Figure out the filename to download to.
	lastSeparatorIndex := strings.LastIndex(url, "/")
	if lastSeparatorIndex == -1 {
		return errors.New("malformed url (no slash!?)")
	}
	filename := url[lastSeparatorIndex+1:]
	if len(filename) == 0 {
		return errors.New("malformed url (no filename)")
	}
	filepath := path.Join(*target, filename)

	// Actually download it.
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func lastRapidStartTime(fromTime time.Time, dayOfWeek int, seconds int) time.Time {
	dayDiff := dayOfWeek - int(fromTime.Weekday())
	if dayDiff > 0 {
		dayDiff -= 7
	}

	if dayDiff == 0 {
		if fromTime.Before(time.Date(fromTime.Year(), fromTime.Month(), fromTime.Day(), 0, 0, seconds, 0, time.Local)) {
			dayDiff -= 7
		}
	}

	return time.Date(
		fromTime.Year(), fromTime.Month(), fromTime.Day()+dayDiff, 0, 0, seconds, 0, time.Local)
}

func nextRapidStartTime(fromTime time.Time, dayOfWeek int, seconds int) time.Time {
	return lastRapidStartTime(fromTime.AddDate(0, 0, 7), dayOfWeek, seconds)
}

func isRapid(fromTime time.Time, dayOfWeek int, seconds int) bool {
	rapidStartTime := lastRapidStartTime(fromTime, dayOfWeek, seconds)
	return fromTime.Equal(rapidStartTime) || (fromTime.After(rapidStartTime) && fromTime.Before(rapidStartTime.Add(time.Duration(*rapidCheckDuration)*time.Second)))
}

func nextCheckTime(lastCheckTime time.Time, dayOfWeek int, seconds int) time.Time {
	var nextCheckTime time.Time

	if isRapid(lastCheckTime, dayOfWeek, seconds) {
		nextCheckTime = lastCheckTime.Add(time.Duration(*rapidCheckInterval) * time.Second)
	} else {
		nextCheckTime = lastCheckTime.Add(time.Duration(*checkInterval) * time.Second)
	}

	nextRapidTime := nextRapidStartTime(lastCheckTime, dayOfWeek, seconds)
	if nextCheckTime.After(nextRapidTime) {
		nextCheckTime = nextRapidTime
	}

	return nextCheckTime
}

func firstCheckTime(startTime time.Time, dayOfWeek int, seconds int) time.Time {
	// Grab info from last rapid start time.
	baseTime := lastRapidStartTime(startTime, dayOfWeek, seconds)
	var currentCheckInterval float64
	if isRapid(startTime, dayOfWeek, seconds) {
		currentCheckInterval = float64(*rapidCheckInterval)
	} else {
		baseTime = baseTime.Add(time.Duration(*rapidCheckDuration) * time.Second)
		currentCheckInterval = float64(*checkInterval)
	}

	// Calculate next check time.
	secondsOffsetFromBase := startTime.Sub(baseTime).Seconds()
	nextCheckOffsetFromBase := currentCheckInterval * math.Ceil(secondsOffsetFromBase/currentCheckInterval)
	nextCheckTime := baseTime.Add(time.Duration(nextCheckOffsetFromBase) * time.Second)

	// Fixup check time if it happens to be after the next rapid start time.
	nextRapidTime := nextRapidStartTime(startTime, dayOfWeek, seconds)
	if nextCheckTime.After(nextRapidTime) {
		nextCheckTime = nextRapidTime
	}

	return nextCheckTime
}

func watchFeed(
	messages chan updatedTitleMessage, name string, feedUrl string, dayOfWeek int, seconds int,
	lastTitle string) {
	log.Printf("[%s] Starting watch.", name)

	var checkTime time.Time
	if *checkImmediate {
		checkTime = time.Now()
	} else {
		checkTime = firstCheckTime(time.Now(), dayOfWeek, seconds)
	}

	// Main loop.
	for {
		// Wait until the next check time.
		time.Sleep(checkTime.Sub(time.Now()))
		checkTime = nextCheckTime(checkTime, dayOfWeek, seconds)

		// Fetch RSS.
		<-requestDelayTicker
		log.Printf("[%s] Checking for new items.", name)
		feed, err := rss.Read(feedUrl)
		if err != nil {
			log.Printf("[%s] Error fetching RSS: %s", name, err)
		} else {
			// Download any new files.
			for i := 0; i < len(feed.Item); i++ {
				if feed.Item[i].Title == lastTitle {
					break
				}

				log.Printf("[%s] Fetching %s.", name, feed.Item[i].Title)
				go func(title string, url string) {
					if *downloadDelay > 0 {
						time.Sleep(time.Duration(*downloadDelay) * time.Second)
					}
					<-requestDelayTicker
					err := downloadUrl(url)
					if err != nil {
						log.Printf("[%s] Error fetching %s: %s", name, url, err)
					} else {
						log.Printf("[%s] Fetched %s.", name, title)
					}
				}(feed.Item[i].Title, feed.Item[i].Link)
			}

			// Update last seen title.
			if len(feed.Item) > 0 {
				newTitle := feed.Item[0].Title
				if lastTitle != newTitle {
					lastTitle = newTitle
					messages <- updatedTitleMessage{name, lastTitle}
				}
			}
		}
	}
}

func main() {
	// Check flags.
	flag.Parse()
	if *target == "" {
		log.Fatal("--target is required.")
	}

	log.Print("Starting rss-downloader.")
	requestDelayTicker = time.Tick(time.Duration(*requestDelay) * time.Second)

	// Connect to database.
	db, err := sql.Open("sqlite3", *dbFilename)
	if err != nil {
		log.Fatalf("Error opening database connection: %s", err)
	}
	defer db.Close()

	// Start watching.
	messages := make(chan updatedTitleMessage)
	rows, err := db.Query("SELECT name, url, dayOfWeek, seconds, lastTitle FROM feeds")
	if err != nil {
		log.Fatalf("Error reading RSS feeds: %s", err)
	}
	for rows.Next() {
		var name string
		var url string
		var dayOfWeek int
		var seconds int
		var lastTitle string

		if err := rows.Scan(&name, &url, &dayOfWeek, &seconds, &lastTitle); err != nil {
			log.Fatalf("Error reading RSS feeds: %s", err)
		}

		go watchFeed(messages, name, url, dayOfWeek, seconds, lastTitle)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error reading RSS feeds: %s", err)
	}

	for {
		msg := <-messages
		_, err := db.Exec(
			"UPDATE feeds SET lastTitle = ? WHERE name = ?", msg.Title, msg.Name)
		if err != nil {
			log.Printf("[%s] Error updating last title: %s", msg.Name, err)
		}

		if len(*updateNotifyUrl) > 0 {
			go func(name string) {
				resp, err := http.PostForm(*updateNotifyUrl, url.Values{"text": {name}})
				if err != nil {
					log.Printf("[%s] Error pushing update notification: %s", name, err)
					return
				}
				resp.Body.Close()
			}(msg.Name)
		}
	}
}
