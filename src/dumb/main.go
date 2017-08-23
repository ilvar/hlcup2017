package main

import (
  "archive/zip"
  "encoding/json"
  "log"
  "fmt"
  "os"
  "strings"
  "io/ioutil"
  "strconv"
  "sort"
  "time"
  "github.com/valyala/fasthttp"
  "sync"
)


type User struct {
  ID         int    `json:"id"`
  Email      string `json:"email"`
  FirstName  string `json:"first_name"`
  LastName   string `json:"last_name"`
  Gender     string `json:"gender"`
  BirthDate  int64  `json:"birth_date"`
}

type Users struct {
  Users      []User `json:"users"`
}

type Location struct {
  ID         int    `json:"id"`
  Distance   int    `json:"distance"`
  City       string `json:"city"`
  Place      string `json:"place"`
  Country    string `json:"country"`
}

type Locations struct {
  Locations  []Location `json:"locations"`
}

type Visit struct {
  ID         int    `json:"id"`
  User       int    `json:"user"`
  Location   int    `json:"location"`
  VisitedAt  int64  `json:"visited_at"`
  Mark       *int   `json:"mark"`
}

type UserVisit struct {
  ID         int
  Place      string
  Country    string
  Distance   int
  Gender     string
  Age        int
  VisitedAt  int64
  Mark       int
}

type UserVisitOut struct {
  Place      string `json:"place"`
  VisitedAt  int64  `json:"visited_at"`
  Mark       int    `json:"mark"`
}

type VisitsType []Visit

type UserVisitsType []UserVisitOut

type Visits struct {
  Visits     VisitsType `json:"visits"`
}

type VisitsOut struct {
  Visits     []UserVisitOut `json:"visits"`
}

func (p User) toString() string {
  return toJson(p)
}

func (p Location) toString() string {
  return toJson(p)
}

func (p Visit) toString() string {
  return toJson(p)
}

func (s UserVisitsType) Len() int {
    return len(s)
}
func (s UserVisitsType) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}
func (s UserVisitsType) Less(i, j int) bool {
    return s[i].VisitedAt < s[j].VisitedAt
}

func toJson(p interface{}) string {
  bytes, err := json.Marshal(p)
  if err != nil {
    fmt.Println(err.Error())
    os.Exit(1)
  }

  return string(bytes)
}

var hlUsers = make(map[string][]byte)
var hlUsersData = make(map[string]User)
var hlUsersEmails = make(map[string]string)
var hlLocations = make(map[string][]byte)
var hlLocationsData = make(map[string]Location)
var hlVisits = make(map[string][]byte)
var hlVisitsData = make(map[string]*Visit)
var hlVisitsByUser = make(map[string][]*Visit)
var hlVisitsByLoc = make(map[string][]*Visit)

var hlUsersMutex sync.Mutex
var hlLocationsMutex sync.Mutex
var hlVisitsMutex sync.Mutex
var hlVisitsByUserMutex sync.Mutex
var hlVisitsByLocMutex sync.Mutex

var emptyResponse = []byte("")

func UserValidate(u User, id string) (bool) {
  if u.Gender != "" && u.Gender != "m" && u.Gender != "f" {
    // Sorry LGBTQ
    return false
  }

  if len(u.FirstName) >= 50 {
    return false
  }

  if len(u.LastName) >= 50 {
    return false
  }

  if len(u.Email) >= 100 {
    return false
  }

  emailErr := ValidateFormat(u.Email)
  if emailErr != nil {
    return false
  }

  if emailID, ok := hlUsersEmails[u.Email]; ok {
    if emailID != id {
      return false
    }
  }

  return true
}

func UsersHandlerPOST(ctx *fasthttp.RequestCtx, id string) (int, []byte) {
  body := ctx.PostBody()

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, emptyResponse
  }

  var u User
  err := json.Unmarshal(body, &u)

  if err != nil {
    return 400, emptyResponse
  }

  if id != "new" {
    existingUser := hlUsersData[id]
    if u.BirthDate == 0 { u.BirthDate = existingUser.BirthDate }
    if u.Gender == "" { u.Gender = existingUser.Gender }
    if u.FirstName == "" { u.FirstName = existingUser.FirstName }
    if u.LastName == "" { u.LastName = existingUser.LastName }
    if u.Email == "" { u.Email = existingUser.Email }
  }

  if UserValidate(u, id) {
    if id != "new" {
      if u.ID != 0 {
        return 400, emptyResponse
      }
      u.ID, _ = strconv.Atoi(id)

      hlUsersMutex.Lock()
      hlUsers[id] = []byte(toJson(u))
      hlUsersData[id] = u
      hlUsersEmails[u.Email] = id
      hlUsersMutex.Unlock()
      return 200, []byte("{}")
    } else {
      newId := strconv.Itoa(u.ID)
      if newId == "0" {
        return 400, emptyResponse
      }

      if _, ok := hlUsers[newId]; ok {
        return 400, emptyResponse
      } else {
        hlUsersMutex.Lock()
        hlUsers[newId] = []byte(toJson(u))
        hlUsersData[newId] = u
        hlUsersEmails[u.Email] = newId
        hlUsersMutex.Unlock()
        return 200, []byte("{}")
      }
    }
  } else {
    return 400, emptyResponse
  }
}

func UsersHandlerGETVisits(ctx *fasthttp.RequestCtx, id string) (int, []byte) {
  visits := hlVisitsByUser[id]
  visitsOut := make([]UserVisitOut, 0)

  params := ctx.QueryArgs()

  if params.Has("fromDate") {
    p0, err := strconv.Atoi(string(params.Peek("fromDate")))
    if err != nil || p0 == 0 {
      return 400, emptyResponse
    }
  }

  if params.Has("toDate") {
    p0, err := strconv.Atoi(string(params.Peek("toDate")))
    if err != nil || p0 == 0 {
      return 400, emptyResponse
    }
  }

  if params.Has("toDistance") {
    p0, err := strconv.Atoi(string(params.Peek("toDistance")))
    if err != nil || p0 == 0 {
      return 400, emptyResponse
    }
  }

  for _, v0 := range visits {
    v := *v0

    shoudlInclude := true

    if shoudlInclude && params.Has("fromDate") {
      p0, _ := strconv.Atoi(string(params.Peek("fromDate")))
      shoudlInclude = shoudlInclude && v.VisitedAt > int64(p0)
    }

    if shoudlInclude && params.Has("toDate") {
      p0, _ := strconv.Atoi(string(params.Peek("toDate")))
      shoudlInclude = shoudlInclude && v.VisitedAt < int64(p0)
    }

    l := hlLocationsData[strconv.Itoa(v.Location)]
    if shoudlInclude && params.Has("country") {
      p0 := string(params.Peek("country"))
      shoudlInclude = shoudlInclude && l.Country == p0
    }

    if shoudlInclude && params.Has("toDistance") {
      p0, _ := strconv.Atoi(string(params.Peek("toDistance")))
      shoudlInclude = shoudlInclude && l.Distance < p0
    }

    if id == "299" {
      println("shoudlInclude")
      println(toJson(v))
      println(shoudlInclude)
    }

    if shoudlInclude {
      uvo := UserVisitOut{l.Place, v.VisitedAt, *v.Mark}
      visitsOut = append(visitsOut, uvo)
    }
  }

  sort.Sort(UserVisitsType(visitsOut))

  vos := VisitsOut{visitsOut}
  return 200, []byte(toJson(vos))
}

func LocationsHandlerPOST(ctx *fasthttp.RequestCtx, id string) (int, []byte) {
  body := ctx.PostBody()

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, emptyResponse
  }

  var l Location
  err := json.Unmarshal(body, &l)

  if err != nil {
    return 400, emptyResponse
  }

  if id != "new" {
    existingLoc := hlLocationsData[id]
    if l.Distance == 0 { l.Distance = existingLoc.Distance }
    if l.Country == "" { l.Country = existingLoc.Country }
    if l.Place == "" { l.Place = existingLoc.Place }
    if l.City == "" { l.City = existingLoc.City }
  }

  if len(l.Country) >= 50 {
    return 400, emptyResponse
  }

  if len(l.City) >= 50 {
    return 400, emptyResponse
  }

  if id != "new" {
    if l.ID != 0 {
      return 400, emptyResponse
    }
    l.ID, _ = strconv.Atoi(id)

    hlLocationsMutex.Lock()
    hlLocationsData[id] = l
    hlLocations[id] = []byte(toJson(l))
    hlLocationsMutex.Unlock()
    return 200, []byte("{}")
  } else {
    locID := strconv.Itoa(l.ID)
    if locID == "0" {
      return 400, emptyResponse
    }

    if _, ok := hlLocations[locID]; ok {
      return 400, emptyResponse
    } else {
      hlLocationsMutex.Lock()
      hlLocationsData[locID] = l
      hlLocations[locID] = []byte(toJson(l))
      hlLocationsMutex.Unlock()
      return 200, []byte("{}")
    }
  }
}

func LocationsHandlerGETAvg(ctx *fasthttp.RequestCtx, id string) (int, []byte) {
  visits := hlVisitsByLoc[id]

  params := ctx.QueryArgs()
  total := 0
  cnt := 0

  if params.Has("fromDate") {
    p0, err := strconv.Atoi(string(params.Peek("fromDate")))
    if err != nil || p0 == 0 {
      return 400, emptyResponse
    }
  }

  if params.Has("toDate") {
    p0, err := strconv.Atoi(string(params.Peek("toDate")))
    if err != nil || p0 == 0 {
      return 400, emptyResponse
    }
  }

  if params.Has("fromAge") {
    p0, err := strconv.Atoi(string(params.Peek("fromAge")))
    if err != nil || p0 == 0 {
      return 400, emptyResponse
    }
  }

  if params.Has("toAge") {
    p0, err := strconv.Atoi(string(params.Peek("toAge")))
    if err != nil || p0 == 0 {
      return 400, emptyResponse
    }
  }

  if params.Has("gender") {
    p0 := string(params.Peek("gender"))
    if p0 != "m" && p0 != "f" {
      return 400, emptyResponse
    }
  }


  for _, v0 := range visits {
    var v Visit = *v0

    if strconv.Itoa(v.Location) != id {
      continue
    }

    shoudlInclude := true

    if params.Has("fromDate") {
      p0, _ := strconv.Atoi(string(params.Peek("fromDate")))
      shoudlInclude = shoudlInclude && v.VisitedAt > int64(p0)
    }

    if params.Has("toDate") {
      p0, _ := strconv.Atoi(string(params.Peek("toDate")))
      shoudlInclude = shoudlInclude && v.VisitedAt < int64(p0)
    }

    u := hlUsersData[strconv.Itoa(v.User)]
    age := Age(time.Unix(u.BirthDate, 0))

    if params.Has("fromAge") {
      p0, _ := strconv.Atoi(string(params.Peek("fromAge")))

      shoudlInclude = shoudlInclude && age >= int(p0)
    }

    if params.Has("toAge") {
      p0, _ := strconv.Atoi(string(params.Peek("toAge")))

      shoudlInclude = shoudlInclude && age < int(p0)
    }

    if params.Has("gender") {
      p0 := string(params.Peek("gender"))
      shoudlInclude = shoudlInclude && u.Gender == p0
    }

    if shoudlInclude {
      total += int(*v.Mark)
      cnt += 1
    }
  }

  avg := float64(total) / float64(cnt)
  if cnt == 0 {
    avg = 0.0
  }
  return 200, []byte("{\"avg\": " + strconv.FormatFloat(avg, 'f', 5, 64) + "}")
}

func VisitsHandlerPOST(ctx *fasthttp.RequestCtx, id string) (int, []byte) {
  body := ctx.PostBody()

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, emptyResponse
  }

  var v Visit
  err := json.Unmarshal(body, &v)

  if id == "10372" {
    println("ID 10372 update Visit")
    println(string(body))
  }

  if err != nil {
    return 400, emptyResponse
  }

  if v.Location > 0 {
    if _, ok := hlLocations[strconv.Itoa(v.Location)]; !ok {
      return 400, emptyResponse
    }
  }

  if v.User > 0 {
    if _, ok := hlUsers[strconv.Itoa(v.User)]; !ok {
      println("Visit " + id + " wrong User: " + strconv.Itoa(v.User))

      return 400, emptyResponse
    }
  }

  if v.Mark != nil && *v.Mark > 5 {
    return 400, emptyResponse
  }

  if v.Mark == nil && id == "new" {
    return 400, emptyResponse
  }

  if id != "new" {
    if v.ID != 0 {
      return 400, emptyResponse
    }

    existingVisit := hlVisitsData[id]
    if id == "10372" {
      println("ID 10372 ptr")
      println(existingVisit)
    }

    if v.VisitedAt != 0 && v.VisitedAt != existingVisit.VisitedAt { existingVisit.VisitedAt = v.VisitedAt }

    if v.Mark != nil && *v.Mark != *existingVisit.Mark { *existingVisit.Mark = *v.Mark }

    if v.Location != 0 && v.Location != existingVisit.Location {
      if id == "10372" {
        println("ID 10372 update Location")
        println(v.Location)
        println(existingVisit.Location)
      }

      removeFromLocations(existingVisit.Location, existingVisit)

      newLoc := strconv.Itoa(v.Location)
      existingVisit.Location = v.Location

      hlVisitsByLocMutex.Lock()
      hlVisitsByLoc[newLoc] = append(hlVisitsByLoc[newLoc], existingVisit)
      hlVisitsByLocMutex.Unlock()
    }
    if v.User != 0 && v.User != existingVisit.User {
      if id == "10372" {
        println("ID 10372 update user")
        println(v.User)
        println(existingVisit.User)
      }

      removeFromUsers(existingVisit.User, existingVisit)

      newUser := strconv.Itoa(v.User)
      existingVisit.User = v.User

      hlVisitsByUserMutex.Lock()
      hlVisitsByUser[newUser] = append(hlVisitsByUser[newUser], existingVisit)
      hlVisitsByUserMutex.Unlock()
    }

    if id == "10372" {
      println("ID 10372 updated")
      println(toJson(existingVisit))
      println(existingVisit)
    }

    hlVisits[id] = []byte(toJson(existingVisit))
    hlVisitsData[id] = existingVisit

    return 200, []byte("{}")
  } else {
    newId := strconv.Itoa(v.ID)
    if newId == "0" {
      println("Visit " + newId + " no ID")
      return 400, emptyResponse
    }

    if _, ok := hlVisits[newId]; ok {
      println("Visit " + id + " existing ID: " + newId)
      return 400, emptyResponse
    } else {
      if v.User == 299 {
        println("User 299 new Visit")
        println(string(body))
      }

      hlVisitsMutex.Lock()
      hlVisits[newId] = []byte(toJson(v))
      hlVisitsData[newId] = &v
      hlVisitsMutex.Unlock()

      userId := strconv.Itoa(v.User)
      hlVisitsByUserMutex.Lock()
      hlVisitsByUser[userId] = append(hlVisitsByUser[userId], &v)
      hlVisitsByUserMutex.Unlock()

      locId := strconv.Itoa(v.Location)
      hlVisitsByLocMutex.Lock()
      hlVisitsByLoc[locId] = append(hlVisitsByLoc[locId], &v)
      hlVisitsByLocMutex.Unlock()
      return 200, []byte("{}")
    }
  }
}

func removeFromLocations(oldId int, existingVisit *Visit) {
  hlVisitsByLocMutex.Lock()

  oldLoc := strconv.Itoa(oldId)

  var oldIdx int = -1

  for i, v0 := range hlVisitsByLoc[oldLoc] {
    if v0.ID == existingVisit.ID {
      oldIdx = i
      break
    }
  }

  if oldIdx > -1 {
    hlVisitsByLoc[oldLoc][oldIdx] = hlVisitsByLoc[oldLoc][len(hlVisitsByLoc[oldLoc])-1]
    hlVisitsByLoc[oldLoc][len(hlVisitsByLoc[oldLoc])-1] = nil
    hlVisitsByLoc[oldLoc] = hlVisitsByLoc[oldLoc][:len(hlVisitsByLoc[oldLoc])-1]
  }

  hlVisitsByLocMutex.Unlock()
}

func removeFromUsers(oldId int, existingVisit *Visit) {
  hlVisitsByUserMutex.Lock()

  oldUser := strconv.Itoa(oldId)

  var oldIdx int = -1

  for i, v0 := range hlVisitsByUser[oldUser] {
    if v0.ID == existingVisit.ID {
      oldIdx = i
      break
    }
  }

  if oldIdx > -1 {
    hlVisitsByUser[oldUser][oldIdx] = hlVisitsByUser[oldUser][len(hlVisitsByUser[oldUser])-1]
    hlVisitsByUser[oldUser][len(hlVisitsByUser[oldUser])-1] = nil
    hlVisitsByUser[oldUser] = hlVisitsByUser[oldUser][:len(hlVisitsByUser[oldUser])-1]
  }

  hlVisitsByUserMutex.Unlock()
}

func GenericHandler(ctx *fasthttp.RequestCtx) {
  path := string(ctx.Path())
  pathBits := strings.Split(path, "/")
  status, body := 400, emptyResponse
  methodPost := ctx.IsPost()

  if len(pathBits) < 3 || len(pathBits) > 4 {
    status, body = 404, emptyResponse
  } else {
    objType := pathBits[1]
    sid := pathBits[2]
    _, err := strconv.Atoi(sid)
    if err != nil {
      if objType == "users" && sid == "new" && methodPost {
        status, body = UsersHandlerPOST(ctx, sid)
      } else if objType == "locations" && sid == "new" && methodPost {
        status, body = LocationsHandlerPOST(ctx, sid)
      } else if objType == "visits" && sid == "new" && methodPost {
        status, body = VisitsHandlerPOST(ctx, sid)
      } else {
        status, body = 404, emptyResponse
      }

    } else {
      if objType == "users" {
        if u, ok := hlUsers[sid]; ok {
          if len(pathBits) == 4 {
            if pathBits[3] == "visits" {
              status, body = UsersHandlerGETVisits(ctx, sid)
            } else {
              status, body = 404, emptyResponse
            }
          } else {
            if methodPost {
              status, body = UsersHandlerPOST(ctx, sid)
            } else {
              status, body = 200, u
            }
          }
        } else {
          status, body = 404, emptyResponse
        }
      } else if objType == "locations" {
        if l, ok := hlLocations[sid]; ok {
          if len(pathBits) == 4 {
            if pathBits[3] == "avg" {
              status, body = LocationsHandlerGETAvg(ctx, sid)
            } else {
              status, body = 404, emptyResponse
            }
          } else {
            if methodPost {
              status, body = LocationsHandlerPOST(ctx, sid)
            } else {
              status, body = 200, l
            }
          }
        } else {
          status, body = 404, emptyResponse
        }
      } else if objType == "visits" {
        if v, ok := hlVisits[sid]; ok {
          if len(pathBits) == 4 {
            status, body = 404, emptyResponse
          } else {
            if methodPost {
              status, body = VisitsHandlerPOST(ctx, sid)
            } else {
              status, body = 200, v
            }
        }
        } else {
          status, body = 404, emptyResponse
        }
      }
    }
  }

  ctx.SetStatusCode(status)
  ctx.Write(body)

  if methodPost {
    ctx.SetConnectionClose()
  }
}

func LoadUsers(r *zip.ReadCloser) {
  start := time.Now()

  for _, f := range r.File {
    if strings.HasPrefix(f.Name, "users_") {
      rc, err := f.Open()
      if err != nil {
        log.Fatal(err)
      }
      byteValue, err := ioutil.ReadAll(rc)
      if err != nil {
        log.Fatal(err)
      }
      var users Users
      json.Unmarshal(byteValue, &users)
      rc.Close()

      for _, v := range users.Users {
        id := strconv.Itoa(v.ID)
        hlUsers[id] = []byte(toJson(v))
        hlUsersData[id] = v
        hlUsersEmails[v.Email] = id
      }

      println("Loaded users: " + strconv.Itoa(len(users.Users)))
    }
  }

  elapsed := time.Since(start)
  log.Printf("LoadUsers took %s", elapsed)
}

func LoadLocations(r *zip.ReadCloser) {
  start := time.Now()

  for _, f := range r.File {
    if strings.HasPrefix(f.Name, "locations_") {
      rc, err := f.Open()
      if err != nil {
        log.Fatal(err)
      }
      byteValue, err := ioutil.ReadAll(rc)
      if err != nil {
        log.Fatal(err)
      }
      var locations Locations
      json.Unmarshal(byteValue, &locations)
      rc.Close()

      for _, v := range locations.Locations {
        hlLocationsData[strconv.Itoa(v.ID)] = v
        hlLocations[strconv.Itoa(v.ID)] = []byte(toJson(v))
      }

      println("Loaded locations: " + strconv.Itoa(len(locations.Locations)))
    }
  }

  elapsed := time.Since(start)
  log.Printf("LoadLocations took %s", elapsed)
}

func LoadVisits(r *zip.ReadCloser) {
  start := time.Now()

  for _, f := range r.File {
    if strings.HasPrefix(f.Name, "visits_") {
      rc, err := f.Open()
      if err != nil {
        log.Fatal(err)
      }
      byteValue, err := ioutil.ReadAll(rc)
      if err != nil {
        log.Fatal(err)
      }
      var visits Visits
      json.Unmarshal(byteValue, &visits)
      rc.Close()

      for _, v0 := range visits.Visits {
        var v Visit = v0

        vID := strconv.Itoa(v.ID)
        hlVisits[vID] = []byte(toJson(v))
        hlVisitsData[vID] = &v

        userId := strconv.Itoa(v.User)
        hlVisitsByUser[userId] = append(hlVisitsByUser[userId], &v)

        locId := strconv.Itoa(v.Location)
        hlVisitsByLoc[locId] = append(hlVisitsByLoc[locId], &v)
      }

      println("Loaded visits: " + strconv.Itoa(len(visits.Visits)))
    }
  }

  elapsed := time.Since(start)
  log.Printf("LoadVisits took %s", elapsed)
}

func main () {
  println("Loading zip...")

  // Open a zip archive for reading.
  r, err := zip.OpenReader("/tmp/data/data.zip")
  if err != nil {
    log.Fatal(err)
  }
  defer r.Close()

  println("Loading data...")

  // Iterate through the files in the archive,
  // printing some of their contents.
  go LoadUsers(r)
  go LoadLocations(r)
  go LoadVisits(r)

  port := os.Getenv("PORT")
  if port == "" {
    port = "80"
  }

  println("Serving http://localhost:" + port + "/ ...")

  server := &fasthttp.Server{
    Name: "X",
    Handler: GenericHandler,
    Concurrency: 1024 * 1024,
    MaxConnsPerIP: 1024 * 1024,
    DisableKeepalive: true,
    LogAllErrors: true}

  server.ListenAndServe(":" + port)
}
