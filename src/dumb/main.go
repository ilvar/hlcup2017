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
  "runtime"
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

var hlUsersData = make(map[int]User)
var hlUsersEmails = make(map[string]int)
var hlLocationsData = make(map[int]Location)
var hlVisitsData = make(map[int]*Visit)
var hlVisitsByUser = make(map[int][]int)
var hlVisitsByLoc = make(map[int][]int)

var hlUsersMutex sync.Mutex
var hlLocationsMutex sync.Mutex
var hlVisitsMutex sync.Mutex
var hlVisitsByUserMutex sync.Mutex
var hlVisitsByLocMutex sync.Mutex

var emptyResponse = []byte("")

func UserValidate(u User, id int) (bool) {
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

func UsersHandlerPOST(ctx *fasthttp.RequestCtx, id int) (int, []byte) {
  body := ctx.PostBody()

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, emptyResponse
  }

  var u User
  err := json.Unmarshal(body, &u)

  if err != nil {
    return 400, emptyResponse
  }

  if id != -1 {
    existingUser := hlUsersData[id]
    if u.BirthDate == 0 { u.BirthDate = existingUser.BirthDate }
    if u.Gender == "" { u.Gender = existingUser.Gender }
    if u.FirstName == "" { u.FirstName = existingUser.FirstName }
    if u.LastName == "" { u.LastName = existingUser.LastName }
    if u.Email == "" { u.Email = existingUser.Email }
  }

  if UserValidate(u, id) {
    if id != -1 {
      if u.ID != 0 {
        return 400, emptyResponse
      }
      u.ID = id

      hlUsersMutex.Lock()
      hlUsersData[id] = u
      hlUsersEmails[u.Email] = id
      hlUsersMutex.Unlock()
      return 200, []byte("{}")
    } else {
      if u.ID == 0 {
        return 400, emptyResponse
      }

      if _, ok := hlUsersData[u.ID]; ok {
        return 400, emptyResponse
      } else {
        hlUsersMutex.Lock()
        hlUsersData[u.ID] = u
        hlUsersEmails[u.Email] = u.ID
        hlUsersMutex.Unlock()
        return 200, []byte("{}")
      }
    }
  } else {
    return 400, emptyResponse
  }
}

func UsersHandlerGETVisits(ctx *fasthttp.RequestCtx, uid int) (int, []byte) {
  visitIds := hlVisitsByUser[uid]
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

  for _, vID := range visitIds {
    v := hlVisitsData[vID]

    shoudlInclude := true

    if shoudlInclude && params.Has("fromDate") {
      p0, _ := strconv.Atoi(string(params.Peek("fromDate")))
      shoudlInclude = shoudlInclude && v.VisitedAt > int64(p0)
    }

    if shoudlInclude && params.Has("toDate") {
      p0, _ := strconv.Atoi(string(params.Peek("toDate")))
      shoudlInclude = shoudlInclude && v.VisitedAt < int64(p0)
    }

    l := hlLocationsData[v.Location]
    if shoudlInclude && params.Has("country") {
      p0 := string(params.Peek("country"))
      shoudlInclude = shoudlInclude && l.Country == p0
    }

    if shoudlInclude && params.Has("toDistance") {
      p0, _ := strconv.Atoi(string(params.Peek("toDistance")))
      shoudlInclude = shoudlInclude && l.Distance < p0
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

func LocationsHandlerPOST(ctx *fasthttp.RequestCtx, id int) (int, []byte) {
  body := ctx.PostBody()

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, emptyResponse
  }

  var l Location
  err := json.Unmarshal(body, &l)

  if err != nil {
    return 400, emptyResponse
  }

  if id != -1 {
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

  if id != -1 {
    if l.ID != 0 {
      return 400, emptyResponse
    }
    l.ID = id

    hlLocationsMutex.Lock()
    hlLocationsData[id] = l
    hlLocationsMutex.Unlock()
    return 200, []byte("{}")
  } else {
    locID := l.ID
    if locID == 0 {
      return 400, emptyResponse
    }

    if _, ok := hlLocationsData[locID]; ok {
      return 400, emptyResponse
    } else {
      hlLocationsMutex.Lock()
      hlLocationsData[locID] = l
      hlLocationsMutex.Unlock()
      return 200, []byte("{}")
    }
  }
}

func LocationsHandlerGETAvg(ctx *fasthttp.RequestCtx, lid int) (int, []byte) {
  visitIDs := hlVisitsByLoc[lid]

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


  for _, vID := range visitIDs {
    v := hlVisitsData[vID]

    shoudlInclude := true

    if params.Has("fromDate") {
      p0, _ := strconv.Atoi(string(params.Peek("fromDate")))
      shoudlInclude = shoudlInclude && v.VisitedAt > int64(p0)
    }

    if params.Has("toDate") {
      p0, _ := strconv.Atoi(string(params.Peek("toDate")))
      shoudlInclude = shoudlInclude && v.VisitedAt < int64(p0)
    }

    u := hlUsersData[v.User]
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

  var avg float64
  if cnt == 0 {
    avg = 0.0
  } else {
    avg = (float64(total) / float64(cnt)) + 0.00005
  }
  return 200, []byte("{\"avg\": " + strconv.FormatFloat(avg, 'f', 5, 64) + "}")
}

func VisitsHandlerPOST(ctx *fasthttp.RequestCtx, id int) (int, []byte) {
  body := ctx.PostBody()

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, emptyResponse
  }

  var v Visit
  err := json.Unmarshal(body, &v)

  if err != nil {
    return 400, emptyResponse
  }

  if v.Location > 0 {
    if _, ok := hlLocationsData[v.Location]; !ok {
      return 400, emptyResponse
    }
  }

  if v.User > 0 {
    if _, ok := hlUsersData[v.User]; !ok {
      return 400, emptyResponse
    }
  }

  if v.Mark != nil && *v.Mark > 5 {
    return 400, emptyResponse
  }

  if v.Mark == nil && id == -1 {
    return 400, emptyResponse
  }

  if id != -1 {
    if v.ID != 0 {
      return 400, emptyResponse
    }

    existingVisit := hlVisitsData[id]

    if v.VisitedAt != 0 && v.VisitedAt != existingVisit.VisitedAt { existingVisit.VisitedAt = v.VisitedAt }

    if v.Mark != nil && *v.Mark != *existingVisit.Mark { *existingVisit.Mark = *v.Mark }

    if v.Location != 0 && v.Location != existingVisit.Location {
      removeFromLocations(existingVisit.Location, existingVisit)

      newLoc := v.Location
      existingVisit.Location = v.Location

      hlVisitsByLocMutex.Lock()
      hlVisitsByLoc[newLoc] = append(hlVisitsByLoc[newLoc], existingVisit.ID)
      hlVisitsByLocMutex.Unlock()
    }
    if v.User != 0 && v.User != existingVisit.User {
      removeFromUsers(existingVisit.User, existingVisit)

      newUser := v.User
      existingVisit.User = v.User

      hlVisitsByUserMutex.Lock()
      hlVisitsByUser[newUser] = append(hlVisitsByUser[newUser], existingVisit.ID)
      hlVisitsByUserMutex.Unlock()
    }

    hlVisitsData[id] = existingVisit

    return 200, []byte("{}")
  } else {
    newId := v.ID
    if newId == 0 {
      return 400, emptyResponse
    }

    if _, ok := hlVisitsData[newId]; ok {
      return 400, emptyResponse
    } else {
      hlVisitsMutex.Lock()
      hlVisitsData[newId] = &v
      hlVisitsMutex.Unlock()

      userId := v.User
      hlVisitsByUserMutex.Lock()
      hlVisitsByUser[userId] = append(hlVisitsByUser[userId], newId)
      hlVisitsByUserMutex.Unlock()

      locId := v.Location
      hlVisitsByLocMutex.Lock()
      hlVisitsByLoc[locId] = append(hlVisitsByLoc[locId], newId)
      hlVisitsByLocMutex.Unlock()
      return 200, []byte("{}")
    }
  }
}

func removeFromLocations(oldId int, existingVisit *Visit) {
  hlVisitsByLocMutex.Lock()

  var oldIdx int = -1

  for i, vID := range hlVisitsByLoc[oldId] {
    if vID == existingVisit.ID {
      oldIdx = i
      break
    }
  }

  if oldIdx > -1 {
    hlVisitsByLoc[oldId][oldIdx] = hlVisitsByLoc[oldId][len(hlVisitsByLoc[oldId])-1]
    hlVisitsByLoc[oldId] = hlVisitsByLoc[oldId][:len(hlVisitsByLoc[oldId])-1]
  }

  hlVisitsByLocMutex.Unlock()
}

func removeFromUsers(oldId int, existingVisit *Visit) {
  hlVisitsByUserMutex.Lock()

  var oldIdx int = -1

  for i, vID := range hlVisitsByUser[oldId] {
    if vID == existingVisit.ID {
      oldIdx = i
      break
    }
  }

  if oldIdx > -1 {
    hlVisitsByUser[oldId][oldIdx] = hlVisitsByUser[oldId][len(hlVisitsByUser[oldId])-1]
    hlVisitsByUser[oldId] = hlVisitsByUser[oldId][:len(hlVisitsByUser[oldId])-1]
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
    iid, err := strconv.Atoi(sid)
    if err != nil {
      if objType == "users" && sid == "new" && methodPost {
        status, body = UsersHandlerPOST(ctx, -1)
      } else if objType == "locations" && sid == "new" && methodPost {
        status, body = LocationsHandlerPOST(ctx, -1)
      } else if objType == "visits" && sid == "new" && methodPost {
        status, body = VisitsHandlerPOST(ctx, -1)
      } else {
        status, body = 404, emptyResponse
      }

    } else {
      if objType == "users" {
        if u, ok := hlUsersData[iid]; ok {
          if len(pathBits) == 4 {
            if pathBits[3] == "visits" {
              status, body = UsersHandlerGETVisits(ctx, iid)
            } else {
              status, body = 404, emptyResponse
            }
          } else {
            if methodPost {
              hlUsersMutex.Lock()
              status, body = UsersHandlerPOST(ctx, iid)
              hlUsersMutex.Unlock()
            } else {
              status, body = 200, []byte(toJson(u))
            }
          }
        } else {
          status, body = 404, emptyResponse
        }
      } else if objType == "locations" {
        if l, ok := hlLocationsData[iid]; ok {
          if len(pathBits) == 4 {
            if pathBits[3] == "avg" {
              status, body = LocationsHandlerGETAvg(ctx, iid)
            } else {
              status, body = 404, emptyResponse
            }
          } else {
            if methodPost {
              hlLocationsMutex.Lock()
              status, body = LocationsHandlerPOST(ctx, iid)
              hlLocationsMutex.Unlock()
            } else {
              status, body = 200, []byte(toJson(l))
            }
          }
        } else {
          status, body = 404, emptyResponse
        }
      } else if objType == "visits" {
        if v, ok := hlVisitsData[iid]; ok {
          if len(pathBits) == 4 {
            status, body = 404, emptyResponse
          } else {
            if methodPost {
              hlVisitsMutex.Lock()
              status, body = VisitsHandlerPOST(ctx, iid)
              hlVisitsMutex.Unlock()
            } else {
              status, body = 200, []byte(toJson(v))
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
        id := v.ID
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
        hlLocationsData[v.ID] = v
      }

      println("Loaded locations: " + strconv.Itoa(len(locations.Locations)))
    }
  }

  elapsed := time.Since(start)
  log.Printf("LoadLocations took %s", elapsed)
}

func LoadVisitsFile(f *zip.File, start time.Time) {
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

  hlVisitsMutex.Lock()
  for _, v0 := range visits.Visits {
    var v Visit = v0

    vID := v.ID
    hlVisitsData[vID] = &v

    userId := v.User
    hlVisitsByUser[userId] = append(hlVisitsByUser[userId], vID)

    locId := v.Location
    hlVisitsByLoc[locId] = append(hlVisitsByLoc[locId], vID)
  }
  hlVisitsMutex.Unlock()

  elapsed := time.Since(start)

  runtime.GC()

  var ms runtime.MemStats
  runtime.ReadMemStats(&ms)

  log.Printf("LoadVisits took %s for %d visits", elapsed, len(visits.Visits))
  log.Printf("Memory: Heap %d mb Total %d mb", ms.Alloc / 1024 / 1024, ms.Sys / 1024 / 1024)
}

func LoadVisits(r *zip.ReadCloser) {
  start := time.Now()

  for _, f := range r.File {
    if strings.HasPrefix(f.Name, "visits_") {
      go LoadVisitsFile(f, start)
    }
  }
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
