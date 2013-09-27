package main

import (
  "database/sql"
  "errors"
  "flag"
  "io"
  "log"
  "net/http"
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
    if fromTime.Sub(time.Date(fromTime.Year(), fromTime.Month(), fromTime.Day(), 0, 0, 0, 0, time.Local)).Seconds() < float64(seconds) {
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
  secondsSinceRapidStart := fromTime.Sub(lastRapidStartTime(fromTime, dayOfWeek, seconds)).Seconds()
  return secondsSinceRapidStart < float64(*rapidCheckDuration)
}

func watchFeed(
  messages chan updatedTitleMessage, name string, feedUrl string, dayOfWeek int, seconds int,
  lastTitle string) {
  log.Printf("[%s] Starting watch.", name)

  checkTime := time.Now()

  // Main loop.
  for {
    log.Printf("[%s] Checking for new items.", name)

    // Fetch RSS.
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
        go func(url string) {
          err := downloadUrl(url)
          if err != nil {
            log.Printf("[%s] Error downloading %s: %s", name, url, err)
          }
        }(feed.Item[i].Link)
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

    // Determine next wait time & wait.
    if isRapid(checkTime, dayOfWeek, seconds) {
      checkTime = checkTime.Add(time.Duration(*rapidCheckInterval) * time.Second)
    } else {
      checkTime = checkTime.Add(time.Duration(*checkInterval) * time.Second)
    }

    nextRapidTime := nextRapidStartTime(checkTime, dayOfWeek, seconds)
    if checkTime.After(nextRapidTime) {
      checkTime = nextRapidTime
    }
    time.Sleep(checkTime.Sub(time.Now()))
  }
}

func main() {
  log.Print("Starting rss-downloader.")

  // Check flags.
  flag.Parse()
  if *target == "" {
    log.Fatal("Flag 'target' is required.")
  }

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
      log.Printf("[%s] Error updating last title: %s", err)
    }
  }
}
