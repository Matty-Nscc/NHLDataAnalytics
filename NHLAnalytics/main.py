import os

from bs4 import BeautifulSoup
import requests
import json
import pyodbc
import datetime
import zipfile
import csv
import calendar
import pandas as pd
# https://docs.python-guide.org/scenarios/scrape/#
# https://gitlab.com/dword4/nhlapi/-/blob/master/stats-api.md
# https://steviehoward.com/NHL-API-docs/

server = "."
database = "NHLData"
username = "sa"
password = "<EnterPassword>"
connectionString = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+password
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()
# Preloaded Data
injuryStatus = []
teams = []
players = []
schedule = []
proLineOdds = None

def downloadTeamsAndCurrentPlayers():
    response = requests.get("https://statsapi.web.nhl.com/api/v1/teams?expand=team.roster")
    j = json.loads(response.content)
    sql = "INSERT INTO dbo.Teams (TeamID,Name,ShortName,Code,VenueID,ConferenceID) VALUES (?, ?, ?, ?, ?, ?)"

    for t in j["teams"]:
        vendorID = venueCheckByName(t["venue"]["name"])
        cursor.execute(sql, t["id"], t["name"], t["shortName"], t["abbreviation"], vendorID, t["conference"]["id"])
        teams.append({"TeamID": t["id"], "Name": t["name"], "ShortName": t["shortName"]})

        for p in t["roster"]["roster"]:
            downloadPlayersByID(p["person"]["id"])

def teamCheckByID(id):
    for team in teams:
        if team["TeamID"] == id:
            return

    response = requests.get("https://statsapi.web.nhl.com/api/v1/teams/" + str(id))
    j = json.loads(response.content)
    vendorID = "0"

    if "venue" in j["teams"][0].keys():
        vendorID = venueCheckByName(j["teams"][0]["venue"]["name"])

    for t in j["teams"]:
        if "id" in t["division"].keys():
            cursor.execute("INSERT INTO dbo.Teams (TeamID,Name,ShortName,Code,VenueID,DivisionID,ConfrenceID) VALUES (?, ?, ?, ?, ?, ?, ?)",t["id"], t["name"], t["shortName"], t["abbreviation"], vendorID, t["division"]["id"], t["conference"]["id"])
            teams.append({"TeamID": id, "Name": t["name"], "ShortName": t["shortName"]})
        else:
            cursor.execute("INSERT INTO dbo.Teams (TeamID,Name,Code,VenueID) VALUES (?, ?, ?, ?)",t["id"], t["name"], t["abbreviation"], vendorID)
            teams.append({"TeamID": id, "Name": t["name"], "ShortName": "" })

def teamIDByName(name):
    for t in teams:
        if t["Name"] == name:
            return t["TeamID"]

    raise Exception('No team with the name ' + name + ' on file.')

def teamIDByShortName(name):
    for t in teams:
        if t["ShortName"] == name:
            return t["TeamID"]

    raise Exception('No team with short code ' + name + ' on file.')

def findPlayerID(name, teamID):
    for p in players:
        if str(p["FullName"]).upper() == name.upper():
            return p["PlayerID"]

    raise Exception('Player ID not on file for ' + name)

def downloadPlayersByID(id):
    response = requests.get("https://statsapi.web.nhl.com/api/v1/people/" + str(id))

    # Was able to get the data from the website
    if response.status_code == 200:
        j = json.loads(response.content)
        sql = "INSERT INTO dbo.Players (PlayerID,FullName,BirthDate,Height,Weight,ShootsCatches,TeamID,PositionID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

        #Determine the players height
        height = 0
        feetInches = str(j["people"][0]["height"]).split(" ")

        if len(feetInches) == 2:
            height = (int(feetInches[0][:1]) * 12) + int(feetInches[1][:len(feetInches[1]) - 1])

        #The player is on a team currently
        if "currentTeam" in j["people"][0]:
            cursor.execute(sql, j["people"][0]["id"], j["people"][0]["fullName"], j["people"][0]["birthDate"],
                       height, j["people"][0]["weight"], j["people"][0]["shootsCatches"],
                       j["people"][0]["currentTeam"]["id"], j["people"][0]["primaryPosition"]["abbreviation"])

            #Add the player to the list
            players.append({"PlayerID": id, "FullName": j["people"][0]["fullName"], "TeamID": j["people"][0]["currentTeam"]["id"]})
        #The player is not on a team currently
        else:
            cursor.execute(sql, j["people"][0]["id"], j["people"][0]["fullName"], j["people"][0]["birthDate"],
                           height, j["people"][0]["weight"], j["people"][0]["shootsCatches"],
                           None, j["people"][0]["primaryPosition"]["abbreviation"])

            # Add the player to the list
            players.append({"PlayerID": id, "FullName": j["people"][0]["fullName"], "TeamID": None })
    else:
        raise Exception("Failed to retrieve a response from the API for downloading player id: " + str(id))

def playerCheck(id):
    for p in players:
        if p["PlayerID"] == id:
            return
    #Download player if it doesn't exist
    downloadPlayersByID(id)

def downloadVenues():
    response = requests.get("https://statsapi.web.nhl.com/api/v1/venues")
    j = json.loads(response.content)

    for v in j["venues"]:
        sql = "INSERT INTO dbo.Venues (VenueID,Name) VALUES (?, ?)"
        cursor.execute(sql, v["id"], v["name"])

def downloadConferences():
    response = requests.get("https://statsapi.web.nhl.com/api/v1/conferences")
    j = json.loads(response.content)

    for c in j["conferences"]:
        sql = "INSERT INTO dbo.Conferences (ConferenceID,Name,Active) VALUES (?, ?, ?)"
        cursor.execute(sql, c["id"], c["name"], int(str(c["active"]) == 'True'))

def downloadDivisions():
    response = requests.get("https://statsapi.web.nhl.com/api/v1/divisions")
    j = json.loads(response.content)

    for d in j["divisions"]:
        sql = "INSERT INTO dbo.Divisions (DivisionID,Name,ConferenceID,Active) VALUES (?, ?, ?, ?)"
        cursor.execute(sql, d["id"], d["name"], d["conference"]["id"], int(str(d["active"]) == 'True'))

def venueCheckByID(id, name):
    return venueCheck(id, name)

def venueCheckByName(name):
    return venueCheck("", name)

def venueCheck(id, name):
    venueID = "0"

    if id != "":
        cursor.execute("SELECT VenueID FROM dbo.Venues WHERE VenueID = " + str(id))
        rows = cursor.fetchall()

        # Doesn't exist
        if len(rows) == 0:
            cursor.execute("INSERT INTO dbo.Venues (VenueID,Name) VALUES (?, ?)", id, str(name).replace("'","''"))
            #cnxn.commit()

        venueID = id
    else:
        cursor.execute("SELECT VenueID FROM dbo.Venues WHERE Name = '" + str(name).replace("'","''") + "'")
        rows = cursor.fetchall()

        # Venue on file
        if len(rows) > 0:
            venueID = rows[0][0]
        else:
            cursor.execute("SELECT COALESCE(MAX(VenueID),0) + 1 AS id FROM dbo.Venues WHERE VenueID < 100")
            rows = cursor.fetchall()
            cursor.execute("INSERT INTO dbo.Venues (VenueID,Name) VALUES (?, ?)", rows[0][0], str(name).replace("'","''"))
            #cnxn.commit()
            venueID = rows[0][0]

    return venueID

def eventTypeCheck(id, description):
    cursor.execute("SELECT EventTypeID FROM dbo.EventType WHERE EventTypeID = '" + str(id) + "'")
    rows = cursor.fetchall()

    # Doesn't exist
    if len(rows) == 0:
        cursor.execute("INSERT INTO dbo.EventType (EventTypeID,Description) VALUES (?, ?)", id, description)

def downloadGamesByDate(date, nextGamePlayID = 0):
    response = requests.get("https://statsapi.web.nhl.com/api/v1/schedule?startDate=" + date + "&endDate=" + date)
    j = json.loads(response.content)

    #Get initial GamePlayID
    if nextGamePlayID < 1:
        results = cursor.execute("SELECT COALESCE(MAX(GamePlayID),0) + 1 FROM dbo.GamePlays")
        for row in results.fetchall():
            nextGamePlayID = row[0]

    for date in j["dates"]:
        for g in date["games"]:
            response = requests.get("https://statsapi.web.nhl.com/api/v1/game/" + str(g["gamePk"]) + "/feed/live")
            gamePlay = json.loads(response.content)
            venueID = "0"
            gameDate = datetime.datetime.strptime(g["gameDate"], '%Y-%m-%dT%H:%M:%SZ')
            # Time difference from UTC time.
            gameDate = gameDate + datetime.timedelta(hours=-4)

            # Make sure the venue exist
            if "id" in g["venue"].keys():
                venueID = venueCheckByID(str(g["venue"]["id"]), g["venue"]["name"])
            else:
                venueID = venueCheckByName(g["venue"]["name"])

            # Make sure the teams exist
            teamCheckByID(g["teams"]["away"]["team"]["id"])
            teamCheckByID(g["teams"]["home"]["team"]["id"])

            cursor.execute("INSERT INTO dbo.Games (GameID,GameType,Date,SeasonID,AwayTeamID,HomeTeamID,VenueID) VALUES (?, ?, ?, ?, ?, ?, ?)",
                           g["gamePk"], g["gameType"], gameDate.strftime('%Y-%m-%d %H:%M:%S'), g["season"], g["teams"]["away"]["team"]["id"], g["teams"]["home"]["team"]["id"], venueID)

            gameSkaters("away", gamePlay, g)
            gameGoalies("away", gamePlay, g)
            gameSkaters("home", gamePlay, g)
            gameGoalies("home", gamePlay, g)

            for play in gamePlay["liveData"]["plays"]["allPlays"]:
                period = play["about"]["period"]  # {int} 1
                periodType = play["about"]["periodType"]
                periodTime = str(play["about"]["periodTime"]).split(":")  # {str} 07:52
                periodTime = (int(periodTime[0]) * 60) + int(periodTime[1])
                eventTypeCheck(play["result"]["eventTypeId"], play["result"]["event"])
                secondaryType = None
                x = None
                y = None

                if periodType == "REGULAR":
                    periodType = "RG"
                elif periodType == "OVERTIME":
                    periodType = "OT"
                elif periodType == "SHOOTOUT":
                    periodType = "SO"
                else:
                    raise Exception("Unknown period in game id: ", str(g["gamePk"]), " period type: ", periodType)

                if("coordinates" in play.keys()):
                    if("x" in play["coordinates"].keys()):
                        x = play["coordinates"]["x"]
                    if ("y" in play["coordinates"].keys()):
                        y = play["coordinates"]["y"]

                if("secondaryType" in play["result"].keys()):
                    secondaryType = play["result"]["secondaryType"]

                cursor.execute("INSERT INTO dbo.GamePlays (GamePlayID, GameID, EventTypeID, SecondaryType, Period, PeriodType, PeriodTime, x, y) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               nextGamePlayID, g["gamePk"], play["result"]["eventTypeId"], secondaryType, period, periodType, periodTime, x, y)

                if play["result"]["eventTypeId"] == "GOAL":
                    emptyNet = 0

                    if("emptyNet" in play["result"].keys() and play["result"]["emptyNet"] == True):
                        emptyNet = 1

                    cursor.execute("INSERT INTO dbo.GamePlayGoals (GamePlayID, EmptyNet) VALUES (?, ?)",
                        nextGamePlayID, emptyNet)
                elif play["result"]["eventTypeId"] == "PENALTY":
                    cursor.execute(
                        "INSERT INTO dbo.GamePlayPenalties (GamePlayID, PenaltySeverity, PenaltyMinutes) VALUES (?, ?, ?)",
                        nextGamePlayID, play["result"]["penaltySeverity"], play["result"]["penaltyMinutes"])

                if "players" in play.keys():
                    penaltyOn = ""

                    for pl in play["players"]:
                        playerCheck(pl["player"]["id"])

                        if pl["playerType"] == "PenaltyOn":
                            penaltyOn = pl["player"]["id"]
                        elif pl["playerType"] == "ServedBy" and penaltyOn == pl["player"]["id"]:
                            continue

                        try:
                            cursor.execute("INSERT INTO dbo.GamePlayPlayers (GamePlayID, PlayerID, PlayerType) VALUES (?, ?, ?)", nextGamePlayID, pl["player"]["id"], pl["playerType"])
                        except Exception as err:
                            raise Exception("Game ID: " + g["gamePk"] + ". " + err)

                nextGamePlayID = nextGamePlayID + 1
    return nextGamePlayID

def downloadNewGames():
    cursor.execute("SELECT CASE WHEN G.MaxDate > COALESCE(NS.StartDate,'1990-12-31') THEN G.MaxDate ELSE NS.StartDate END AS 'NextDate' FROM (SELECT MAX(Date) AS 'MaxDate' FROM dbo.Games) G LEFT JOIN dbo.Seasons S ON S.EndDate = G.MaxDate LEFT JOIN dbo.Seasons NS ON NS.SeasonID = S.SeasonID + 10001")
    date = cursor.fetchall()

    if(len(date) == 1):
        date = datetime.date(date[0][0].year, date[0][0].month, date[0][0].day) + datetime.timedelta(days=1)
        endDate = datetime.date.today()

        while (date < endDate):
            downloadGamesByDate(date.strftime('%Y-%m-%d'))
            date = date + datetime.timedelta(days=1)
    else:
        raise Exception("Failed to find max date or there is no game date.")

def gameSkaters(side, gamePlays, game):
    for playerID in gamePlays["liveData"]["boxscore"]["teams"][side]["skaters"]:
        player = gamePlays["liveData"]["boxscore"]["teams"][side]["players"]["ID" + str(playerID)]

        if "skaterStats" in player["stats"].keys():
            ps = player["stats"]["skaterStats"]
            eTOI = str(ps["evenTimeOnIce"]).split(":")
            ppTOI = str(ps["powerPlayTimeOnIce"]).split(":")
            shTOI = str(ps["shortHandedTimeOnIce"]).split(":")

            cursor.execute("INSERT INTO dbo.GamePlayers (GameID,TeamID,PlayerID,PositionID,EvenTOI,PowerPlayTOI,ShortHandedTOI) VALUES (?, ?, ?, ?, ?, ?, ?)",
                           game["gamePk"], gamePlays["liveData"]["boxscore"]["teams"][side]["team"]["id"], playerID,
                           player["position"]["abbreviation"],
                           (int(eTOI[0]) * 60) + int(eTOI[1]), (int(ppTOI[0]) * 60) + int(ppTOI[1]),
                           (int(shTOI[0]) * 60) + int(shTOI[1]))

def gameGoalies(side, gamePlays, game):
    for playerID in gamePlays["liveData"]["boxscore"]["teams"][side]["goalies"]:
        player = gamePlays["liveData"]["boxscore"]["teams"][side]["players"]["ID" + str(playerID)]

        if "goalieStats" in player["stats"].keys():
            ps = player["stats"]["goalieStats"]
            TOI = str(ps["timeOnIce"]).split(":")

            cursor.execute("INSERT INTO dbo.GameGoalies (GameID,TeamID,PlayerID,TOI) VALUES (?, ?, ?, ?)",
                           game["gamePk"], gamePlays["liveData"]["boxscore"]["teams"][side]["team"]["id"], playerID,
                           (int(TOI[0]) * 60) + int(TOI[1]))

def downloadScheduleToday():
    today = datetime.date.today().strftime("%Y-%m-%d")
    response = requests.get("https://statsapi.web.nhl.com/api/v1/schedule?startDate=" + today + "&endDate=" + today)
    j = json.loads(response.content)
    schedule.clear()

    # Purge old schedule
    cursor.execute("DELETE dbo.Schedule")

    for d in j["dates"]:
        for g in d["games"]:
            sql = "INSERT INTO dbo.Schedule (ScheduleID,Date,SeasonID,AwayTeamID,HomeTeamID,VenueID) VALUES (?, ?, ?, ?, ?, ?)"
            gameDate = datetime.datetime.strptime(g["gameDate"], '%Y-%m-%dT%H:%M:%SZ')
            # Time difference from UTC time.
            gameDate = gameDate + datetime.timedelta(hours=-4)

            if "id" in g["venue"].keys():
                cursor.execute(sql, g["gamePk"], gameDate.strftime("%Y-%m-%d %H:%M:%S"), g["season"], g["teams"]["away"]["team"]["id"], g["teams"]["home"]["team"]["id"],g["venue"]["id"])
            else:
                cursor.execute(sql, g["gamePk"], gameDate.strftime("%Y-%m-%d %H:%M:%S"), g["season"], g["teams"]["away"]["team"]["id"], g["teams"]["home"]["team"]["id"], venueCheckByName(g["venue"]["name"]))

            schedule.append({ "GameID": g["gamePk"], "Date": gameDate.strftime("%Y-%m-%d %H:%M:%S"), "AwayTeamID": g["teams"]["away"]["team"]["id"], "HomeTeamID": g["teams"]["home"]["team"]["id"]})

    dowloadProLineOdds()

def checkInjuryStatus(name):
    StatusID = 0

    #Load the Statuses
    if len(injuryStatus) == 0:
        cursor.execute("SELECT StatusID, Description FROM dbo.PlayerInjuryStatuses")
        rows = cursor.fetchall()

        for r in rows:
            injuryStatus.append({ "StatusID": r[0], "Description": r[1] })

    for status in injuryStatus:
        if status["Description"] == name:
            return status["StatusID"]

    if len(injuryStatus) > 0:
        StatusID = int(injuryStatus[-1]["StatusID"]) + 1
    else:
        StatusID = 1

    cn = pyodbc.connect(connectionString)
    cr = cn.cursor()

    cr.execute("INSERT INTO dbo.PlayerInjuryStatuses (StatusID,Description) VALUES (?, ?)", StatusID, name)
    cr.commit()

    #Add to the onfile injury statuses
    injuryStatus.append({ "StatusID": StatusID, "Description": name })

    return StatusID

def dowloadInjuries():
    cursor.execute("SELECT PlayerID, Date FROM dbo.PlayerInjuries WHERE RecoveredDate IS NULL ORDER BY Date")
    rows = cursor.fetchall()
    page = requests.get('https://www.espn.com/nhl/injuries')

    #Was able to get the data from the website
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, "html.parser")
        result = soup.find_all(class_="ResponsiveTable Table__league-injuries")
        openInjuries = []

        for row in rows:
            openInjuries.append({"PlayerID":row[0], "Date":row[1], "OnFile":False })

        for html in result:
            teamID = teamIDByName(str(html.contents[0].contents[0].contents[1].contents[0]))

            for playerContent in html.contents[1].contents[0].contents[1].contents[0].contents[2]:
                playerID = findPlayerID(str(playerContent.contents[0].contents[0].contents[0]).replace("  ", " "), teamID)
                dateParts = str(playerContent.contents[2].contents[0]).split(" ")
                date = datetime.date(2021, list(calendar.month_abbr).index(dateParts[0]), int(dateParts[1]))
                statusID = checkInjuryStatus(playerContent.contents[3].contents[0].contents[0])
                comment = None
                onfile = False

                if len(playerContent.contents[4]) > 0:
                    comment = playerContent.contents[4].contents[0]

                #Mark an open injury as still active
                for i in range(len(openInjuries)):
                    if playerID == openInjuries[i]["PlayerID"] and date == openInjuries[i]["Date"]:
                        onfile = True
                        openInjuries[i]["OnFile"] = True
                        break

                if not onfile:
                    cursor.execute("INSERT INTO dbo.PlayerInjuries (PlayerID,Date,StatusID,Comment) VALUES (?, ?, ?, ?)", playerID, date.strftime('%Y-%m-%d'), statusID, comment)

        for injury in openInjuries:
            if injury["OnFile"] == False:
                cursor.execute("UPDATE dbo.PlayerInjuries SET RecoveredDate = CONVERT(VARCHAR(8),GETDATE(),112) WHERE PlayerID = ? AND Date = ?", injury["PlayerID"], injury["Date"])
    else:
        raise Exception('Failed to download Injury data from the web. Try again later...')

def dowloadProLineOdds():
    page = requests.get('https://www.alc.ca/portal?action=GoGamesList&amp;coupon=PRO-LINE')

    # Was able to get the data from the website
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, "html.parser")
        result = soup.find_all(class_="panel-default")
        date = datetime.datetime.now()
        dateStr = date.strftime('%Y-%m-%d')
        global proLineOdds
        newOdds = pd.DataFrame(columns=["MatchID","AwayTeamID","HomeTeamID"])

        for html in result:
            if html.contents[0].contents[0].contents[0].contents[0].contents[0] == "PRO HOCKEY":
                games = html.find_all(class_="proline-game-row")

                for game in games:
                    dateparts = str(game.contents[0].contents[3].contents[0]).split(" ")
                    #Only get todays prolines
                    if date.strftime('%b %d') == dateparts[0] + " " + dateparts[1]:
                        matchID = int(str(game.contents[0].contents[1].attrs["href"]).split("=")[-1])
                        awayTeam = teamIDByShortName(game.contents[1].contents[0].contents[0].contents[0])
                        homeTeam = teamIDByShortName(game.contents[1].contents[0].contents[4].contents[0])
                        gameID = None

                        for gameSchedule in schedule:
                            if gameSchedule["AwayTeamID"] == awayTeam and gameSchedule["HomeTeamID"] == homeTeam:
                                gameID = gameSchedule["GameID"]

                        #Money Line
                        mAwayOdds = str(game.contents[2].contents[0].contents[0]).split(" ")[-1]
                        mTieOdds = str(game.contents[2].contents[1].contents[0]).split(" ")[-1]
                        mHomeOdds = str(game.contents[2].contents[2].contents[0]).split(" ")[-1]

                        #Spreads
                        sValue = float(str(game.contents[3].contents[0].contents[0]).split(" ")[1])
                        sAwayOdd = float(str(game.contents[3].contents[0].contents[0]).split(" ")[2].lstrip("(").rstrip(")"))
                        sHomeOdd = float(str(game.contents[3].contents[1].contents[0]).split(" ")[2].lstrip("(").rstrip(")"))

                        #Total
                        tValue = float(str(game.contents[4].contents[0].contents[0]).split(" ")[1])
                        tOverOdd = float(str(game.contents[4].contents[0].contents[0]).split(" ")[2].lstrip("(").rstrip(")"))
                        tUnderOdd = float(str(game.contents[4].contents[1].contents[0]).split(" ")[2].lstrip("(").rstrip(")"))

                        #Match already downloaded, update odds
                        if matchID in proLineOdds.MatchID.values:
                            cursor.execute("UPDATE dbo.ProLineOdds SET TieOdds = ?,SpreadValue = ?,TotalValue = ?,OverOdds = ?,UnderOdds = ?,GameID = ?, LastUpdated = ? WHERE MatchID = ?",
                                mTieOdds, sValue, tValue, tOverOdd, tUnderOdd, gameID, date.strftime("%Y-%m-%d %X"), matchID)
                            cursor.execute("UPDATE dbo.ProLineMoneyLineTeams SET Odds = ? WHERE MatchID = ? AND TeamID = ?",
                                mAwayOdds, matchID, awayTeam)
                            cursor.execute("UPDATE dbo.ProLineMoneyLineTeams SET Odds = ? WHERE MatchID = ? AND TeamID = ?",
                                mHomeOdds, matchID, homeTeam)
                            cursor.execute("UPDATE dbo.ProLineSpreadsTeams SET Odds = ? WHERE MatchID = ? AND TeamID = ?",
                                sAwayOdd, matchID, awayTeam)
                            cursor.execute("UPDATE dbo.ProLineSpreadsTeams SET Odds = ? WHERE MatchID = ? AND TeamID = ?",
                                sHomeOdd, matchID, homeTeam)
                        else:
                            cursor.execute("INSERT INTO dbo.ProLineOdds (MatchID,Date,AwayTeamID,HomeTeamID,TieOdds,SpreadValue,TotalValue,OverOdds,UnderOdds,GameID,LastUpdated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                matchID, dateStr, awayTeam, homeTeam, mTieOdds, sValue, tValue, tOverOdd, tUnderOdd, gameID, date.strftime("%Y-%m-%d %X"))
                            cursor.execute("INSERT INTO dbo.ProLineMoneyLineTeams (MatchID,TeamID,Odds) VALUES (?, ?, ?)",
                                matchID, awayTeam, mAwayOdds)
                            cursor.execute("INSERT INTO dbo.ProLineMoneyLineTeams (MatchID,TeamID,Odds) VALUES (?, ?, ?)",
                                matchID, homeTeam, mHomeOdds)
                            cursor.execute("INSERT INTO dbo.ProLineSpreadsTeams (MatchID,TeamID,Odds) VALUES (?, ?, ?)",
                                matchID, awayTeam, sAwayOdd)
                            cursor.execute("INSERT INTO dbo.ProLineSpreadsTeams (MatchID,TeamID,Odds) VALUES (?, ?, ?)",
                                matchID, homeTeam, sHomeOdd)

                            newOdds = newOdds.append(pd.DataFrame([[matchID, awayTeam, homeTeam]],columns=["MatchID","AwayTeamID","HomeTeamID"]))

            elif html.contents[0].contents[0].contents[0].contents[0].contents[0] == "HOCKEY: WINNING MARGIN":
                matches = html.find_all(class_="outright-game-row")

                for match in matches:
                    dateparts = str(match.contents[0].contents[0].contents[0].contents[0].contents[0].contents[0].contents[4]).split(" ")
                    # Only get todays prolines
                    if date.strftime('%b %d') == dateparts[1] + " " + dateparts[2]:
                        matchID = int(str(match.contents[0].contents[0].contents[0].contents[0].contents[0].contents[0].contents[2].attrs["onclick"]).split("=")[3].split("'")[0])

                        matchOdds = match.find_all(class_="outright-game-row-item")
                        odds = { "01":0, "02":0, "03":0, "04":0, "05":0, "06":0, "07":0, "08":0 }

                        # 1 Through 8, 1-4 visitor and 5 - 8 home
                        for odd in matchOdds:
                            odds[odd.contents[0].contents[0]] = float(odd.contents[2].contents[0])

                        matchDF = newOdds[newOdds.MatchID.values == matchID]

                        # Match already downloaded, update odds
                        if len(matchDF) == 1:
                            cursor.execute("INSERT INTO dbo.ProLineMarginTeams (MatchID,TeamID,ByThreePlusOdds,ByTwoOdds,ByOneInRegOdds,OverTimeShootOutOdds) VALUES (?,?,?,?,?,?)",
                                matchID,int(matchDF.iloc[0,1]),odds["01"],odds["02"],odds["03"],odds["04"])
                            cursor.execute("INSERT INTO dbo.ProLineMarginTeams (MatchID,TeamID,ByThreePlusOdds,ByTwoOdds,ByOneInRegOdds,OverTimeShootOutOdds) VALUES (?,?,?,?,?,?)",
                                matchID, int(matchDF.iloc[0,2]), odds["08"], odds["07"], odds["06"], odds["05"])
                        else:
                            matchDF = proLineOdds[proLineOdds.MatchID.values == matchID]

                            cursor.execute("UPDATE dbo.ProLineMarginTeams SET ByThreePlusOdds = ?, ByTwoOdds = ?, ByOneInRegOdds = ?, OverTimeShootOutOdds = ? WHERE MatchID = ? AND TeamID = ?",
                                odds["01"],odds["02"],odds["03"],odds["04"], matchID, int(matchDF.iloc[0,1]))
                            cursor.execute(
                                "UPDATE dbo.ProLineMarginTeams SET ByThreePlusOdds = ?, ByTwoOdds = ?, ByOneInRegOdds = ?, OverTimeShootOutOdds = ? WHERE MatchID = ? AND TeamID = ?",
                                odds["08"], odds["07"], odds["06"], odds["05"], matchID, int(matchDF.iloc[0, 2]))

        proLineOdds = proLineOdds.append(newOdds)
    else:
        raise Exception("Failed to load Pro Line betting odds. Try again later.")

def downloadShots(season):
    fileZip = "shots_" + season[:4] + ".zip"
    fileCsv = "shots_" + season[:4] + ".csv"
    currDir = os.getcwd()

    response = requests.get("https://peter-tanner.com/moneypuck/downloads/" + fileZip)
    open(fileZip, 'wb').write(response.content)

    with zipfile.ZipFile(currDir + '\\' + fileZip,'r') as zip_ref:
        zip_ref.extractall(currDir)

    with open(fileCsv, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            print(row)

def downloadSeasonofGameData(season):
    cursor.execute("SELECT StartDate, EndDate FROM dbo.Seasons WHERE SeasonID = " + season)
    rows = cursor.fetchall()
    nextGamePlayID = 0

    for s in rows:
        dateStart = s[0]
        dateEnd = s[1]

        if dateEnd is None:
            dateEnd = datetime.datetime.now().date() - datetime.timedelta(days=1)

        numsOfDays = dateEnd - dateStart
        for day in [dateStart + datetime.timedelta(days=x) for x in range(numsOfDays.days + 1)]:
            nextGamePlayID = downloadGamesByDate(day.strftime('%Y-%m-%d'), nextGamePlayID)

def firstDownload():
    downloadConferences()
    downloadDivisions()
    downloadVenues()
    downloadTeamsAndCurrentPlayers()

def preLoadData():
    #Load Teams
    cursor.execute("SELECT TeamID, Name, ShortName FROM dbo.Teams")
    rows = cursor.fetchall()

    for r in rows:
        teams.append({ "TeamID": r[0], "Name": r[1], "ShortName": r[2] })

    #Load Players
    cursor.execute("SELECT PlayerID, FullName, TeamID FROM dbo.Players")
    rows = cursor.fetchall()

    for r in rows:
        players.append({ "PlayerID": r[0], "FullName": r[1], "TeamID": r[2] })

    #LoadProline Odds
    global proLineOdds
    proLineOdds = pd.read_sql_query("SELECT MatchID, AwayTeamID, HomeTeamID FROM dbo.ProLineOdds WHERE Date = '" + datetime.date.today().strftime("%Y-%m-%d") + "'",
                                    cnxn, dtype={"MatchID":"Int64","AwayTeamID":"Int64","HomeTeamID":"Int64"})

    print("Pre Load Complete\n")

def intro():
    print("Press 1 for first pull of the day.")
    print("Press 2 to download today's schedule and updated Pro Line odds.")
    print("Press 3 to download a player.")
    print("Press 4 to download a season of games.")
    print("Press 5 to download date range of games.")
    print("Press 6 to quit.\n")

if __name__ == '__main__':
    preLoadData()
    intro()

    userInput = input("What would you like to do: ")
    jobStarted = datetime.datetime.now()

    while userInput != "6":
        if userInput == "1":
            downloadNewGames()
            dowloadInjuries()
            downloadScheduleToday()
            cursor.commit()
            jobDiff = datetime.datetime.now() - jobStarted
            print("Job Completed in {} minutes and {} seconds.\n".format(jobDiff.seconds // 60, jobDiff.seconds % 60))
        elif userInput == "2":
            downloadScheduleToday()
            cursor.commit()
            jobDiff = datetime.datetime.now() - jobStarted
            print("Job Completed in {} minutes and {} seconds.\n".format(jobDiff.seconds // 60, jobDiff.seconds % 60))
        elif userInput == "3":
            playerID = input("Player ID: ")
            downloadPlayersByID(playerID)
            cursor.commit()
            jobDiff = datetime.datetime.now() - jobStarted
            print("Job Completed in {} minutes and {} seconds.\n".format(jobDiff.seconds // 60, jobDiff.seconds % 60))
        elif userInput == "4":
            seasonID = input("Season ID: ")
            downloadSeasonofGameData(seasonID)
            cursor.commit()
            jobDiff = datetime.datetime.now() - jobStarted
            print("Job Completed in {} minutes and {} seconds.\n".format(jobDiff.seconds // 60, jobDiff.seconds % 60))
        elif userInput == "5":
            sYear = input("Start Year: ")
            sMonth = input("Start Month: ")
            sDay = input("Start Day: ")
            eYear = input("End Year: ")
            eMonth = input("End Month: ")
            eDay = input("End Day: ")

            date = datetime.date(int(sYear), int(sMonth), int(sDay))
            endDate = datetime.date(int(eYear), int(eMonth), int(eDay))

            while (date < endDate):
                downloadGamesByDate(date.strftime('%Y-%m-%d'))
                date = date + datetime.timedelta(days=1)

            cursor.commit()
            jobDiff = datetime.datetime.now() - jobStarted
            print("Job Completed in {} minutes and {} seconds.\n".format(jobDiff.seconds // 60, jobDiff.seconds % 60))
        else:
            print("I don't understand that command.\n")

        intro()
        userInput = input("What would you like to do: ")
        jobStarted = datetime.datetime.now()

    cursor.close()
    print("Good Bye!")

