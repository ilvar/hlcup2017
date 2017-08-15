package main

import (
  "net/http"
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
  "bytes"
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
  Mark       int    `json:"mark"`
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

var hlUsers = make(map[string]string)
var hlUsersData = make(map[string]User)
var hlUsersEmails = make(map[string]string)
var hlLocations = make(map[string]string)
var hlLocationsData = make(map[string]Location)
var hlVisits = make(map[string]string)
var hlVisitsData = make(map[string]Visit)
//var hlVisitsByUser = make(map[string][]UserVisit)
//var hlVisitsByLocation = make(map[string][]UserVisit)

// TODO: Validate unique emails
// TODO: Chain update UVs on Location change

func UserValidate(u User, id string) (bool) {
  if u.BirthDate > 0 && (u.BirthDate < -1262304000 || u.BirthDate > 915148800) {
    println("User " + id + " wrong Birthdate: " + strconv.Itoa(int(u.BirthDate)))
    return false
  }

  if u.Gender != "" && u.Gender != "m" && u.Gender != "f" {
    println("User " + id + " wrong Gender: " + u.Gender)

    // Sorry LGBTQ
    return false
  }

  if len(u.FirstName) >= 50 {
    println("User " + id + " wrong FirstName: " + u.FirstName)

    return false
  }

  if len(u.LastName) >= 50 {
    println("User " + id + " wrong LastName: " + u.LastName)

    return false
  }

  if len(u.Email) >= 100 {
    println("User " + id + " wrong Email: " + u.Email)

    return false
  }

  emailErr := ValidateFormat(u.Email)
  if emailErr != nil {
    println("User " + id + " wrong Email: " + u.Email)

    return false
  }

  if emailID, ok := hlUsersEmails[u.Email]; ok {
    if emailID != id {
      println("User " + id + " existing Email: " + u.Email)

      return false
    }
  }

  return true
}

func UsersHandlerPOST(request *http.Request, id string) (int, []byte) {
  body, _ := ioutil.ReadAll(request.Body)
  request.Body = ioutil.NopCloser(bytes.NewBuffer(body))

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, []byte("")
  }

  decoder := json.NewDecoder(request.Body)
  var u User
  err := decoder.Decode(&u)
  defer request.Body.Close()
  if err != nil {
    return 400, []byte("")
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
        println("User " + id + " wrong ID: " + strconv.Itoa(u.ID))
        return 400, []byte("")
      }
      u.ID, _ = strconv.Atoi(id)
      hlUsers[id] = toJson(u)
      hlUsersData[id] = u
      hlUsersEmails[u.Email] = id
      return 200, []byte("{}")
    } else {
      newId := strconv.Itoa(u.ID)
      if newId == "0" {
        println("User " + newId + " no ID")
        return 400, []byte("")
      }

      if _, ok := hlUsers[newId]; ok {
        println("User " + id + " existing ID: " + newId)
        return 400, []byte("")
      } else {
        hlUsers[newId] = toJson(u)
        hlUsersData[newId] = u
        hlUsersEmails[u.Email] = newId
        return 200, []byte("{}")
      }
    }
  } else {
    return 400, []byte("")
  }
}

func UsersHandlerGETVisits(request *http.Request, id string) (int, []byte) {
  visits := hlVisitsData
  visitsOut := make([]UserVisitOut, 0)

  params := request.URL.Query()

  if p, ok := params["fromDate"]; ok {
    p0, err := strconv.Atoi(p[0])
    if err != nil || p0 == 0 {
      println("User Visits bad fromDate: " + p[0] + " " + strconv.Itoa(len(p)))
      return 400, []byte("")
    }
  }

  if p, ok := params["toDate"]; ok {
    p0, err := strconv.Atoi(p[0])
    if err != nil || p0 == 0 {
      return 400, []byte("")
    }
  }

  if p, ok := params["toDistance"]; ok {
    p0, err := strconv.Atoi(p[0])
    if err != nil || p0 == 0 {
      return 400, []byte("")
    }
  }

  for _, v := range visits {
    userId := strconv.Itoa(v.User)
    if userId != id {
      continue
    }

    shoudlInclude := true

    if p, ok := params["fromDate"]; ok {
      p0, _ := strconv.Atoi(p[0])
      shoudlInclude = shoudlInclude && v.VisitedAt > int64(p0)
    }

    if p, ok := params["toDate"]; ok {
      p0, _ := strconv.Atoi(p[0])
      shoudlInclude = shoudlInclude && v.VisitedAt < int64(p0)
    }

    l := hlLocationsData[strconv.Itoa(v.Location)]
    if p, ok := params["country"]; ok {
      shoudlInclude = shoudlInclude && l.Country == p[0]
    }

    if p, ok := params["toDistance"]; ok {
      p0, _ := strconv.Atoi(p[0])
      shoudlInclude = shoudlInclude && l.Distance < p0
    }

    if shoudlInclude {
      uvo := UserVisitOut{l.Place, v.VisitedAt, v.Mark}
      visitsOut = append(visitsOut, uvo)
    }
  }

  sort.Sort(UserVisitsType(visitsOut))

  vos := VisitsOut{visitsOut}
  return 200, []byte(toJson(vos))
}

func UsersHandlerGET(id string) (int, []byte) {
  return 200, []byte(hlUsers[id])
}

func LocationsHandlerPOST(request *http.Request, id string) (int, []byte) {
  body, _ := ioutil.ReadAll(request.Body)
  request.Body = ioutil.NopCloser(bytes.NewBuffer(body))

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, []byte("")
  }

  decoder := json.NewDecoder(request.Body)
  var l Location
  err := decoder.Decode(&l)
  defer request.Body.Close()

  if err != nil {
    if id == "132" {
      println("Location " + id + " err: " + err.Error())
    }

    return 400, []byte("")
  }

  if id == "132" {
    println("Location " + id + " json: " + toJson(l))
  }

  if id != "new" {
    existingLoc := hlLocationsData[id]
    if l.Distance == 0 { l.Distance = existingLoc.Distance }
    if l.Country == "" { l.Country = existingLoc.Country }
    if l.Place == "" { l.Place = existingLoc.Place }
    if l.City == "" { l.City = existingLoc.City }
  }

  if id == "132" {
    println("Location " + id + " updated json: " + toJson(l))
  }

  if len(l.Country) >= 50 {
    println("Location " + id + " wrong Country: " + l.Country)
    return 400, []byte("")
  }

  if len(l.City) >= 50 {
    println("Location " + id + " wrong City: " + l.City)
    return 400, []byte("")
  }

  if id != "new" {
    if l.ID != 0 {
      println("Location " + id + " wrong ID: " + strconv.Itoa(l.ID))
      return 400, []byte("")
    }
    l.ID, _ = strconv.Atoi(id)

    if id == "132" {
      println("Location " + id + " saving json: " + toJson(l))
    }

    hlLocationsData[strconv.Itoa(l.ID)] = l
    hlLocations[strconv.Itoa(l.ID)] = toJson(l)
    return 200, []byte("{}")
  } else {
    locID := strconv.Itoa(l.ID)
    if locID == "0" {
      println("Location " + locID + " no ID")
      return 400, []byte("")
    }

    if _, ok := hlLocations[locID]; ok {
      println("Location " + locID + " existing ID: " + locID)
      return 400, []byte("")
    } else {
      hlLocationsData[locID] = l
      hlLocations[locID] = toJson(l)
      return 200, []byte("{}")
    }
  }
}

func LocationsHandlerGETAvg(request *http.Request, id string) (int, []byte) {
  visits := hlVisitsData

  params := request.URL.Query()
  total := 0
  cnt := 0

  if p, ok := params["fromDate"]; ok {
    p0, err := strconv.Atoi(p[0])
    if err != nil || p0 == 0 {
      return 400, []byte("")
    }
  }

  if p, ok := params["toDate"]; ok {
    p0, err := strconv.Atoi(p[0])
    if err != nil || p0 == 0 {
      return 400, []byte("")
    }
  }

  if p, ok := params["fromAge"]; ok {
    p0, err := strconv.Atoi(p[0])
    if err != nil || p0 == 0 {
      return 400, []byte("")
    }
  }

  if p, ok := params["toAge"]; ok {
    p0, err := strconv.Atoi(p[0])
    if err != nil || p0 == 0 {
      return 400, []byte("")
    }
  }

  if p, ok := params["gender"]; ok {
    if p[0] != "m" && p[0] != "f" {
      return 400, []byte("")
    }
  }


  for _, v := range visits {
    if strconv.Itoa(v.Location) != id {
      continue
    }

    shoudlInclude := true

    if p, ok := params["fromDate"]; ok {
      p0, _ := strconv.Atoi(p[0])
      shoudlInclude = shoudlInclude && v.VisitedAt > int64(p0)
    }

    if p, ok := params["toDate"]; ok {
      p0, _ := strconv.Atoi(p[0])
      shoudlInclude = shoudlInclude && v.VisitedAt < int64(p0)
    }

    u := hlUsersData[strconv.Itoa(v.User)]
    age := Age(time.Unix(u.BirthDate, 0))

    if p, ok := params["fromAge"]; ok {
      p0, _ := strconv.Atoi(p[0])

      shoudlInclude = shoudlInclude && age >= int(p0)
    }

    if p, ok := params["toAge"]; ok {
      p0, _ := strconv.Atoi(p[0])

      shoudlInclude = shoudlInclude && age < int(p0)
    }

    if p, ok := params["gender"]; ok {
      shoudlInclude = shoudlInclude && u.Gender == p[0]
    }

    if shoudlInclude {
      total += int(v.Mark)
      cnt += 1
    }
  }

  avg := float64(total) / float64(cnt)
  if cnt == 0 {
    avg = 0.0
  }
  return 200, []byte("{\"avg\": " + strconv.FormatFloat(avg, 'f', 5, 64) + "}")
}

func LocationsHandlerGET(id string) (int, []byte) {
  return 200, []byte(hlLocations[id])
}

func VisitInvalidate(v Visit) {
  return

  //userId := strconv.Itoa(v.User)
  //user := hlUsersData[userId]
  //locId := strconv.Itoa(v.Location)
  //loc := hlLocationsData[locId]
  //userAge := Age(time.Unix(user.BirthDate, 0))
  //uv := UserVisit{v.ID, loc.Place, loc.Country, loc.Distance, user.Gender, userAge, v.VisitedAt, v.Mark}
  //
  //hlVisitsByUser[userId] = append(hlVisitsByUser[userId], uv)
  //sort.Sort(UserVisitsType(hlVisitsByUser[userId]))
  //
  //hlVisitsByLocation[locId] = append(hlVisitsByLocation[locId], uv)

  // TODO: user/location change
}

func VisitsHandlerPOST(request *http.Request, id string) (int, []byte) {
  body, _ := ioutil.ReadAll(request.Body)
  request.Body = ioutil.NopCloser(bytes.NewBuffer(body))

  if strings.Contains(string(body), "null") { // TODO: hack :(
    return 400, []byte("")
  }

  decoder := json.NewDecoder(request.Body)
  var v Visit
  err := decoder.Decode(&v)
  defer request.Body.Close()
  if err != nil {
    return 400, []byte("")
  }

  if id != "new" {
    existingVisit := hlVisitsData[id]
    if v.Location == 0 { v.Location = existingVisit.Location }
    if v.User == 0 { v.User = existingVisit.User }
    if v.VisitedAt == 0 { v.VisitedAt = existingVisit.VisitedAt }
    if v.Mark == 0 { v.Mark = existingVisit.Mark }
  }

  if _, ok := hlLocations[strconv.Itoa(v.Location)]; !ok {
    println("Visit " + id + " wrong Location: " + strconv.Itoa(v.Location))

    return 400, []byte("")
  }

  if _, ok := hlUsers[strconv.Itoa(v.User)]; !ok {
    println("Visit " + id + " wrong User: " + strconv.Itoa(v.User))

    return 400, []byte("")
  }

  if v.VisitedAt < 946684800 || v.VisitedAt > 1420070400 {
    println("Visit " + id + " wrong VisitedAt: " + strconv.Itoa(int(v.VisitedAt)))

    return 400, []byte("")
  }

  if v.Mark < 0 || v.Mark > 5 {
    println("Visit " + id + " wrong Mark: " + strconv.Itoa(v.Mark))

    return 400, []byte("")
  }

  if id != "new" {
    if v.ID != 0 {
      println("Visit " + id + " wrong ID: " + strconv.Itoa(v.ID))
      return 400, []byte("")
    }
    v.ID, _ = strconv.Atoi(id)
    hlVisits[id] = toJson(v)
    hlVisitsData[id] = v
    VisitInvalidate(v)
    return 200, []byte("{}")
  } else {
    newId := strconv.Itoa(v.ID)
    if newId == "0" {
      println("Visit " + newId + " no ID")
      return 400, []byte("")
    }

    if _, ok := hlVisits[newId]; ok {
      println("Visit " + id + " existing ID: " + newId)
      return 400, []byte("")
    } else {
      hlVisits[newId] = toJson(v)
      hlVisitsData[newId] = v
      VisitInvalidate(v)
      return 200, []byte("{}")
    }
  }
}

func VisitsHandlerGET(id string) (int, []byte) {
  return 200, []byte(hlVisits[id])
}

func GenericHandler(writer http.ResponseWriter, request *http.Request) {
  pathBits := strings.Split(request.URL.Path, "/")
  status, body := 400, []byte("")
  if len(pathBits) < 3 || len(pathBits) > 4 {
    status, body = 404, []byte("")
  } else {
    objType := pathBits[1]
    sid := pathBits[2]
    _, err := strconv.Atoi(sid)
    if err != nil {
      if objType == "users" && sid == "new" && request.Method == "POST" {
        status, body = UsersHandlerPOST(request, sid)
      } else if objType == "locations" && sid == "new" && request.Method == "POST" {
        status, body = LocationsHandlerPOST(request, sid)
      } else if objType == "visits" && sid == "new" && request.Method == "POST" {
        status, body = VisitsHandlerPOST(request, sid)
      } else {
        status, body = 404, []byte("")
      }

    } else {
      if objType == "users" {
        if _, ok := hlUsers[sid]; ok {
          if len(pathBits) == 4 {
            if pathBits[3] == "visits" {
              status, body = UsersHandlerGETVisits(request, sid)
            } else {
              status, body = 404, []byte("")
            }
          } else {
            if request.Method == "POST" {
              status, body = UsersHandlerPOST(request, sid)
            } else {
              status, body = UsersHandlerGET(sid)
            }
          }
        } else {
          status, body = 404, []byte("")
        }
      } else if objType == "locations" {
        if _, ok := hlLocations[sid]; ok {
          if len(pathBits) == 4 {
            if pathBits[3] == "avg" {
              status, body = LocationsHandlerGETAvg(request, sid)
            } else {
              status, body = 404, []byte("")
            }
          } else {
            if request.Method == "POST" {
              status, body = LocationsHandlerPOST(request, sid)
            } else {
              status, body = LocationsHandlerGET(sid)
            }
          }
        } else {
          status, body = 404, []byte("")
        }
      } else if objType == "visits" {
        if _, ok := hlVisits[sid]; ok {
          if len(pathBits) == 4 {
            status, body = 404, []byte("")
          } else {
            if request.Method == "POST" {
              status, body = VisitsHandlerPOST(request, sid)
            } else {
              status, body = VisitsHandlerGET(sid)
            }
        }
        } else {
          status, body = 404, []byte("")
        }
      }
    }
  }

  writer.WriteHeader(status)
  writer.Write(body)
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
        hlUsers[id] = toJson(v)
        hlUsersData[id] = v
        hlUsersEmails[v.Email] = id
      }

      println("Loaded users: " + strconv.Itoa(len(users.Users)))
    }
  }

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
        hlLocations[strconv.Itoa(v.ID)] = toJson(v)
      }

      println("Loaded locations: " + strconv.Itoa(len(locations.Locations)))
    }
  }

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

      for _, v := range visits.Visits {
        hlVisits[strconv.Itoa(v.ID)] = toJson(v)
        hlVisitsData[strconv.Itoa(v.ID)] = v

        VisitInvalidate(v)
      }

      println("Loaded visits: " + strconv.Itoa(len(visits.Visits)))
    }
  }
  http.HandleFunc("/", GenericHandler)

  port := os.Getenv("PORT")
  if port == "" {
    port = "80"
  }

  println("Serving http://localhost:" + port + "/ ...")

  http.ListenAndServe(":" + port, nil)
}
