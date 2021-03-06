USE [NHLData]
GO
/****** Object:  Table [dbo].[ProLineOdds]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ProLineOdds](
	[MatchID] [int] NOT NULL,
	[Date] [date] NOT NULL,
	[AwayTeamID] [int] NOT NULL,
	[HomeTeamID] [int] NOT NULL,
	[TieOdds] [decimal](3, 2) NOT NULL,
	[SpreadValue] [decimal](3, 2) NOT NULL,
	[TotalValue] [decimal](3, 2) NOT NULL,
	[OverOdds] [decimal](3, 2) NOT NULL,
	[UnderOdds] [decimal](3, 2) NOT NULL,
	[GameID] [int] NULL,
	[LastUpdated] [datetime] NOT NULL,
 CONSTRAINT [PK_ProLineOdds] PRIMARY KEY CLUSTERED 
(
	[MatchID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ProLineMoneyLineTeams]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ProLineMoneyLineTeams](
	[MatchID] [int] NOT NULL,
	[TeamID] [int] NOT NULL,
	[Odds] [decimal](3, 2) NOT NULL,
 CONSTRAINT [PK_ProLineMoneyLineTeams] PRIMARY KEY CLUSTERED 
(
	[MatchID] ASC,
	[TeamID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ProLineSpreadsTeams]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ProLineSpreadsTeams](
	[MatchID] [int] NOT NULL,
	[TeamID] [int] NOT NULL,
	[Odds] [decimal](3, 2) NOT NULL,
 CONSTRAINT [PK_ProLineSpreadsTeams] PRIMARY KEY CLUSTERED 
(
	[MatchID] ASC,
	[TeamID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Teams]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Teams](
	[TeamID] [int] NOT NULL,
	[Name] [varchar](100) NOT NULL,
	[Code] [varchar](3) NOT NULL,
	[VenueID] [int] NULL,
	[ConferenceID] [int] NULL,
	[ShortName] [varchar](20) NULL,
 CONSTRAINT [PK_Teams] PRIMARY KEY CLUSTERED 
(
	[TeamID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[VI_ProLineOdds]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VI_ProLineOdds] AS
SELECT P.MatchID, P.Date, TA.Name AS 'AwayTeam', TH.Name AS 'HomeTeam', MA.Odds AS 'AwayMLOdds', P.TieOdds, MH.Odds AS 'HomeMLOdds', P.SpreadValue, SA.Odds AS 'AwaySpreadOdds', SH.Odds AS 'HomeSpreadOdds', P.TotalValue, P.OverOdds, P.UnderOdds, P.GameID
FROM dbo.ProLineOdds P
LEFT JOIN dbo.Teams TA ON
P.AwayTeamID = TA.TeamID
LEFT JOIN dbo.Teams TH ON
P.HomeTeamID = TH.TeamID
LEFT JOIN ProLineMoneyLineTeams MA ON
P.MatchID = MA.MatchID AND
P.AwayTeamID = MA.TeamID
LEFT JOIN ProLineMoneyLineTeams MH ON
P.MatchID = MH.MatchID AND
P.HomeTeamID = MH.TeamID
LEFT JOIN ProLineSpreadsTeams SA ON
P.MatchID = SA.MatchID AND
P.AwayTeamID = SA.TeamID
LEFT JOIN ProLineSpreadsTeams SH ON
P.MatchID = SH.MatchID AND
P.HomeTeamID = SH.TeamID
GO
/****** Object:  Table [dbo].[Games]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Games](
	[GameID] [int] NOT NULL,
	[Date] [datetime] NOT NULL,
	[SeasonID] [int] NOT NULL,
	[AwayTeamID] [int] NOT NULL,
	[HomeTeamID] [int] NOT NULL,
	[VenueID] [int] NOT NULL,
	[GameType] [varchar](2) NULL,
 CONSTRAINT [PK_Games] PRIMARY KEY CLUSTERED 
(
	[GameID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[VI_GameTeamRestDays]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VI_GameTeamRestDays] AS
WITH TeamGames AS (
SELECT G.SeasonID, G.TeamID, ROW_NUMBER() OVER (PARTITION BY SeasonID, TeamID ORDER BY TeamID, Date) AS 'GameNum', G.GameID, G.Date
FROM
(SELECT SeasonID, GameID, Date, AwayTeamID AS 'TeamID'
FROM dbo.Games
UNION ALL
SELECT SeasonID, GameID, Date, HomeTeamID
FROM dbo.Games) G)

SELECT C.TeamID, C.GameID, COALESCE(DATEDIFF(Day, L.Date, C.Date), 7) AS 'RestDays'
FROM TeamGames C
LEFT JOIN TeamGames L ON
C.SeasonID = L.SeasonID AND
C.TeamID = L.TeamID AND 
C.GameNum = L.GameNum + 1
GO
/****** Object:  Table [dbo].[GamePlays]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[GamePlays](
	[GamePlayID] [int] NOT NULL,
	[GameID] [int] NOT NULL,
	[EventTypeID] [varchar](20) NOT NULL,
	[SecondaryType] [varchar](50) NULL,
	[Period] [tinyint] NOT NULL,
	[PeriodType] [varchar](2) NOT NULL,
	[PeriodTime] [smallint] NOT NULL,
	[x] [int] NULL,
	[y] [int] NULL,
 CONSTRAINT [PK_GamePlays] PRIMARY KEY CLUSTERED 
(
	[GamePlayID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[GamePlayPlayers]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[GamePlayPlayers](
	[GamePlayID] [int] NOT NULL,
	[PlayerID] [int] NOT NULL,
	[PlayerType] [varchar](30) NOT NULL,
 CONSTRAINT [PK_GamePlayPlayers] PRIMARY KEY CLUSTERED 
(
	[GamePlayID] ASC,
	[PlayerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[GamePlayGoals]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[GamePlayGoals](
	[GamePlayID] [int] NOT NULL,
	[EmptyNet] [bit] NOT NULL,
 CONSTRAINT [PK_GamePlayGoals] PRIMARY KEY CLUSTERED 
(
	[GamePlayID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[GamePlayers]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[GamePlayers](
	[GameID] [int] NOT NULL,
	[TeamID] [int] NOT NULL,
	[PlayerID] [int] NOT NULL,
	[PositionID] [varchar](3) NOT NULL,
	[EvenTOI] [int] NOT NULL,
	[PowerPlayTOI] [int] NOT NULL,
	[ShortHandedTOI] [int] NOT NULL,
 CONSTRAINT [PK_GamePlayers] PRIMARY KEY CLUSTERED 
(
	[GameID] ASC,
	[TeamID] ASC,
	[PlayerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[VI_GameSkaterStats]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [dbo].[VI_GameSkaterStats] 
WITH SCHEMABINDING AS
WITH GPlays AS (
SELECT GP.GameID, GP.EventTypeID, GPP.PlayerID, GPP.PlayerType, COUNT(*) AS 'Times'
FROM dbo.GamePlays GP
INNER JOIN dbo.GamePlayPlayers GPP ON
GP.GamePlayID = GPP.GamePlayID
WHERE GP.EventTypeID <> 'GOAL' AND GPP.PlayerType NOT IN ('Scorer','Goalie')
GROUP BY GP.GameID, GP.EventTypeID, GPP.PlayerID, GPP.PlayerType),

GPlayGoals AS (
SELECT GP.GameID, GPP.PlayerID, GPG.EmptyNet, COUNT(*) AS 'Times'
FROM dbo.GamePlays GP
INNER JOIN dbo.GamePlayPlayers GPP ON
GP.GamePlayID = GPP.GamePlayID
INNER JOIN dbo.GamePlayGoals GPG ON
GPP.GamePlayID = GPG.GamePlayID
WHERE GPP.PlayerType = 'Scorer'
GROUP BY GP.GameID, GPP.PlayerID, GPG.EmptyNet)


SELECT G.GameID, G.Date, GP.TeamID, GP.PlayerID, GP.PositionID, 
GP.EvenTOI + GP.ShortHandedTOI + GP.PowerPlayTOI AS 'TOI', 
GP.EvenTOI, GP.ShortHandedTOI, GP.PowerPlayTOI,
COALESCE(Goals.Times, 0) AS 'Goals', 
COALESCE(GoalsEmpty.Times, 0) AS 'GoalsEmpty', 
COALESCE(Shots.Times,0) AS 'Shots',
COALESCE(Assists.Times,0) AS 'Assists',
COALESCE(FaceOffWon.Times,0) AS 'FaceOffWon',
COALESCE(FaceOffLost.Times,0) AS 'FaceOffLost',
COALESCE(MissedShots.Times,0) AS 'MissedShots',
COALESCE(BlockedShots.Times,0) AS 'BlockedShots',
COALESCE(Hitters.Times,0) AS 'Hitters',
COALESCE(Hittees.Times,0) AS 'Hittees',
COALESCE(TakeAways.Times,0) AS 'TakeAways',
COALESCE(GiveAway.Times,0) AS 'GiveAway',
COALESCE(Penalty.Times,0) AS 'Penalty',
COALESCE(PenaltyDrewBy.Times,0) AS 'PenaltyDrewBy'

FROM dbo.Games G
INNER JOIN dbo.GamePlayers GP ON
G.GameID = GP.GameID

LEFT JOIN GPlayGoals Goals ON
GP.GameID = Goals.GameID AND
GP.PlayerID = Goals.PlayerID AND
Goals.EmptyNet = 0

LEFT JOIN GPlayGoals GoalsEmpty ON
GP.GameID = GoalsEmpty.GameID AND
GP.PlayerID = GoalsEmpty.PlayerID AND
GoalsEmpty.EmptyNet = 1


LEFT JOIN GPlays Shots ON
GP.GameID = Shots.GameID AND
GP.PlayerID = Shots.PlayerID AND
Shots.EventTypeID = 'SHOT' AND
Shots.PlayerType = 'Shooter'

LEFT JOIN GPlays Assists ON
GP.GameID = Assists.GameID AND
GP.PlayerID = Assists.PlayerID AND
Assists.EventTypeID = 'GOAL' AND
Assists.PlayerType = 'Assist'

LEFT JOIN GPlays FaceOffWon ON
GP.GameID = FaceOffWon.GameID AND
GP.PlayerID = FaceOffWon.PlayerID AND
FaceOffWon.EventTypeID = 'FACEOFF' AND
FaceOffWon.PlayerType = 'Winner'

LEFT JOIN GPlays FaceOffLost ON
GP.GameID = FaceOffLost.GameID AND
GP.PlayerID = FaceOffLost.PlayerID AND
FaceOffLost.EventTypeID = 'FACEOFF' AND
FaceOffLost.PlayerType = 'Loser'

LEFT JOIN GPlays MissedShots ON
GP.GameID = MissedShots.GameID AND
GP.PlayerID = MissedShots.PlayerID AND
MissedShots.EventTypeID = 'MISSED_SHOT' AND
MissedShots.PlayerType = 'Shooter'

LEFT JOIN GPlays BlockedShots ON
GP.GameID = BlockedShots.GameID AND
GP.PlayerID = BlockedShots.PlayerID AND
BlockedShots.EventTypeID = 'BLOCKED_SHOT' AND
BlockedShots.PlayerType = 'Shooter'

LEFT JOIN GPlays Hitters ON
GP.GameID = Hitters.GameID AND
GP.PlayerID = Hitters.PlayerID AND
Hitters.EventTypeID = 'HIT' AND
Hitters.PlayerType = 'Hitter'

LEFT JOIN GPlays Hittees ON
GP.GameID = Hittees.GameID AND
GP.PlayerID = Hittees.PlayerID AND
Hittees.EventTypeID = 'HIT' AND
Hittees.PlayerType = 'Hittee'

LEFT JOIN GPlays TakeAways ON
GP.GameID = TakeAways.GameID AND
GP.PlayerID = TakeAways.PlayerID AND
TakeAways.EventTypeID = 'TAKEAWAY' AND
TakeAways.PlayerType = 'PlayerID'

LEFT JOIN GPlays GiveAway ON
GP.GameID = GiveAway.GameID AND
GP.PlayerID = GiveAway.PlayerID AND
GiveAway.EventTypeID = 'GIVEAWAY' AND
GiveAway.PlayerType = 'PlayerID'

LEFT JOIN GPlays Penalty ON
GP.GameID = Penalty.GameID AND
GP.PlayerID = Penalty.PlayerID AND
Penalty.EventTypeID = 'PENALTY' AND
Penalty.PlayerType = 'PenaltyOn'

LEFT JOIN GPlays PenaltyDrewBy ON
GP.GameID = Penalty.GameID AND
GP.PlayerID = Penalty.PlayerID AND
Penalty.EventTypeID = 'PENALTY' AND
Penalty.PlayerType = 'DrewBy'

--WHERE G.GameID = 2014020001

/*
--SELECT * FROM dbo.GamePlays WHERE GameID = 2014020001
SELECT * FROM dbo.GamePlays, dbo.GamePlayPlayers WHERE GamePlays.GamePlayID = GamePlayPlayers.GamePlayID AND GamePlays.GameID = 2014020001
*/
GO
/****** Object:  Table [dbo].[GameGoalies]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[GameGoalies](
	[GameID] [int] NOT NULL,
	[TeamID] [int] NOT NULL,
	[PlayerID] [int] NOT NULL,
	[TOI] [int] NOT NULL,
 CONSTRAINT [PK_GameGoalies] PRIMARY KEY CLUSTERED 
(
	[GameID] ASC,
	[TeamID] ASC,
	[PlayerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[VI_GameGoalieStats]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VI_GameGoalieStats] AS
WITH GPlays AS (
SELECT GP.GameID, GP.EventTypeID, GPP.PlayerID, GPP.PlayerType, COUNT(*) AS 'Times'
FROM dbo.GamePlays GP
INNER JOIN dbo.GamePlayPlayers GPP ON
GP.GamePlayID = GPP.GamePlayID
GROUP BY GP.GameID, GP.EventTypeID, GPP.PlayerID, GPP.PlayerType)

SELECT G.GameID, GP.TeamID, GP.PlayerID, GP.TOI,
COALESCE(Goals.Times, 0) AS 'Goals', 
COALESCE(Shots.Times,0) AS 'Shots',
COALESCE(MissedShots.Times,0) AS 'MissedShots',
COALESCE(BlockedShots.Times,0) AS 'BlockedShots',
COALESCE(Hitters.Times,0) AS 'Hitters',
COALESCE(Hittees.Times,0) AS 'Hittees',
COALESCE(TakeAways.Times,0) AS 'TakeAways',
COALESCE(GiveAway.Times,0) AS 'GiveAway',
COALESCE(Penalty.Times,0) AS 'Penalty',
COALESCE(PenaltyDrewBy.Times,0) AS 'PenaltyDrewBy'

FROM dbo.Games G
INNER JOIN dbo.GameGoalies GP ON
G.GameID = GP.GameID

LEFT JOIN GPlays Goals ON
GP.GameID = Goals.GameID AND
GP.PlayerID = Goals.PlayerID AND
Goals.EventTypeID = 'GOAL' AND
Goals.PlayerType = 'Goalie'

LEFT JOIN GPlays Shots ON
GP.GameID = Shots.GameID AND
GP.PlayerID = Shots.PlayerID AND
Shots.EventTypeID = 'SHOT' AND
Shots.PlayerType = 'Goalie'

LEFT JOIN GPlays MissedShots ON
GP.GameID = MissedShots.GameID AND
GP.PlayerID = MissedShots.PlayerID AND
MissedShots.EventTypeID = 'MISSED_SHOT' AND
MissedShots.PlayerType = 'Unknown'

LEFT JOIN GPlays BlockedShots ON
GP.GameID = BlockedShots.GameID AND
GP.PlayerID = BlockedShots.PlayerID AND
BlockedShots.EventTypeID = 'BLOCKED_SHOT' AND
BlockedShots.PlayerType = 'Shooter'

LEFT JOIN GPlays Hitters ON
GP.GameID = Hitters.GameID AND
GP.PlayerID = Hitters.PlayerID AND
Hitters.EventTypeID = 'HIT' AND
Hitters.PlayerType = 'Hitter'

LEFT JOIN GPlays Hittees ON
GP.GameID = Hittees.GameID AND
GP.PlayerID = Hittees.PlayerID AND
Hittees.EventTypeID = 'HIT' AND
Hittees.PlayerType = 'Hittee'

LEFT JOIN GPlays TakeAways ON
GP.GameID = TakeAways.GameID AND
GP.PlayerID = TakeAways.PlayerID AND
TakeAways.EventTypeID = 'TAKEAWAY' AND
TakeAways.PlayerType = 'PlayerID'

LEFT JOIN GPlays GiveAway ON
GP.GameID = GiveAway.GameID AND
GP.PlayerID = GiveAway.PlayerID AND
GiveAway.EventTypeID = 'GIVEAWAY' AND
GiveAway.PlayerType = 'PlayerID'

LEFT JOIN GPlays Penalty ON
GP.GameID = Penalty.GameID AND
GP.PlayerID = Penalty.PlayerID AND
Penalty.EventTypeID = 'PENALTY' AND
Penalty.PlayerType = 'PenaltyOn'

LEFT JOIN GPlays PenaltyDrewBy ON
GP.GameID = Penalty.GameID AND
GP.PlayerID = Penalty.PlayerID AND
Penalty.EventTypeID = 'PENALTY' AND
Penalty.PlayerType = 'DrewBy'

--WHERE G.GameID = 2014020001

/*
--SELECT * FROM dbo.GamePlays WHERE GameID = 2014020001
SELECT GamePlays.*, GamePlayPlayers.*
FROM dbo.GamePlays, dbo.GamePlayPlayers, dbo.GameGoalies
WHERE 
GamePlays.GamePlayID = GamePlayPlayers.GamePlayID AND 
GameGoalies.GameID = GamePlays.GameID AND
dbo.GamePlayPlayers.PlayerID = GameGoalies.PlayerID
AND GamePlays.EventTypeID NOT IN ('SHOT','GOAL','GIVEAWAY','TAKEAWAY','MISSED_SHOT')
*/
/*
SELECT GamePlays.EventTypeID--, GamePlayPlayers.*
FROM dbo.GamePlays, dbo.GamePlayPlayers, dbo.GameGoalies
WHERE 
GamePlays.GamePlayID = GamePlayPlayers.GamePlayID AND 
GameGoalies.GameID = GamePlays.GameID AND
dbo.GamePlayPlayers.PlayerID = GameGoalies.PlayerID
GROUP by GamePlays.EventTypeID
*/
GO
/****** Object:  View [dbo].[VI_GamePlayerRestDays]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VI_GamePlayerRestDays] AS
WITH PlayerGames AS (
SELECT G.SeasonID, G.GameID, G.Date, GP.PlayerID, ROW_NUMBER() OVER (PARTITION BY G.SeasonID, GP.PlayerID ORDER BY GP.PlayerID, G.Date) AS 'GameNum', 
GP.EvenTOI + GP.PowerPlayTOI + GP.ShortHandedTOI AS 'TOI'
FROM dbo.Games G
INNER JOIN dbo.GamePlayers GP ON
G.GameID = GP.GameID)

SELECT C.GameID, C.PlayerID, C.TOI, L.TOI AS 'LastTOI',
COALESCE(DATEDIFF(Day, L.Date, C.Date), 7) AS 'RestDays'
FROM PlayerGames C
LEFT JOIN PlayerGames L ON
C.SeasonID = L.SeasonID AND
C.PlayerID = L.PlayerID AND 
C.GameNum = L.GameNum + 1
GO
/****** Object:  UserDefinedFunction [dbo].[FN_PlayerStatsTest]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Matthew Veinot
-- Create date: December 18th, 2021
-- Description:	Figure out the stats for players in the game.
-- =============================================
CREATE FUNCTION [dbo].[FN_PlayerStatsTest]
(
	@GameID	int,
	@Date	Date
)
RETURNS TABLE 
AS
RETURN (
	SELECT G.GameID, GP.TeamID, GP.PlayerID, SUM(GSS.TOI) TOI, SUM(GSS.Goals) Goals, SUM(GSS.GoalsEmpty) GoalsEmpty,
	SUM(GSS.Shots) Shots, SUM(GSS.Assists) Assists, SUM(GSS.FaceOffWon) FaceOffWon, SUM(GSS.FaceOffLost) FaceOffLost, 
	SUM(GSS.MissedShots) MissedShots, SUM(GSS.BlockedShots) BlockedShots, SUM(GSS.Hitters) Hitters, 
	SUM(GSS.Hittees) Hittees, SUM(GSS.TakeAways) TakeAways, SUM(GSS.GiveAway) GiveAway, 
	SUM(GSS.Penalty) Penalty, SUM(GSS.PenaltyDrewBy) PenaltyDrewBy, COUNT(*) NumGames
	FROM dbo.Games G 
	INNER JOIN dbo.GamePlayers GP ON
	G.GameID = GP.GameID
	INNER JOIN dbo.VI_GameSkaterStats GSS ON
	GP.PlayerID = GSS.PlayerID
	WHERE G.GameID = @GameID AND G.SeasonID > G.SeasonID - 10001 AND GSS.Date < @Date
	GROUP BY G.GameID, GP.TeamID, GP.PlayerID
)
GO
/****** Object:  Table [dbo].[Schedule]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Schedule](
	[ScheduleID] [int] NOT NULL,
	[Date] [datetime] NOT NULL,
	[SeasonID] [int] NOT NULL,
	[AwayTeamID] [int] NOT NULL,
	[HomeTeamID] [int] NOT NULL,
	[VenueID] [int] NULL,
 CONSTRAINT [PK_Schedule] PRIMARY KEY CLUSTERED 
(
	[ScheduleID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Venues]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Venues](
	[VenueID] [int] NOT NULL,
	[Name] [varchar](100) NOT NULL,
	[Country] [varchar](2) NULL,
	[Province] [varchar](30) NULL,
	[City] [varchar](30) NULL,
 CONSTRAINT [PK_Venues] PRIMARY KEY CLUSTERED 
(
	[VenueID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[VI_Schedule]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [dbo].[VI_Schedule] AS
SELECT S.ScheduleID, S.Date, TA.Name AS 'AwayTeam', TH.Name AS 'HomeTeam', V.Name AS 'VenueName'
FROM dbo.Schedule S
LEFT JOIN dbo.Teams TA ON
S.AwayTeamID = TA.TeamID
LEFT JOIN dbo.Teams TH ON
S.HomeTeamID = TH.TeamID
LEFT JOIN dbo.Venues V ON
S.VenueID = V.VenueID
GO
/****** Object:  Table [dbo].[Players]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Players](
	[PlayerID] [int] NOT NULL,
	[FullName] [varchar](80) NOT NULL,
	[BirthDate] [date] NULL,
	[Height] [int] NULL,
	[Weight] [int] NULL,
	[ShootsCatches] [varchar](1) NULL,
	[TeamID] [int] NULL,
	[PositionID] [varchar](3) NOT NULL,
 CONSTRAINT [PK_Players] PRIMARY KEY CLUSTERED 
(
	[PlayerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PlayerInjuryStatuses]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PlayerInjuryStatuses](
	[StatusID] [int] NOT NULL,
	[Description] [varchar](20) NOT NULL,
 CONSTRAINT [PK_PlayerInjuryStatuses] PRIMARY KEY CLUSTERED 
(
	[StatusID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[PlayerInjuries]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[PlayerInjuries](
	[PlayerID] [int] NOT NULL,
	[Date] [date] NOT NULL,
	[StatusID] [int] NOT NULL,
	[Comment] [varchar](500) NULL,
	[RecoveredDate] [date] NULL,
 CONSTRAINT [PK_PlayerInjuries] PRIMARY KEY CLUSTERED 
(
	[PlayerID] ASC,
	[Date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[VI_PlayerInjuries]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
  CREATE VIEW [dbo].[VI_PlayerInjuries] AS
  SELECT P.PlayerID, P.FullName, PI.Date, PI.RecoveredDate, PIS.Description, PI.Comment
  FROM dbo.PlayerInjuries PI
  INNER JOIN dbo.PlayerInjuryStatuses PIS ON
  PI.StatusID = PIS.StatusID
  INNER JOIN dbo.Players P ON
  PI.PlayerID = P.PlayerID
GO
/****** Object:  Table [dbo].[GamePlayPenalties]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[GamePlayPenalties](
	[GamePlayID] [int] NOT NULL,
	[PenaltySeverity] [varchar](15) NOT NULL,
	[PenaltyMinutes] [int] NULL,
 CONSTRAINT [PK_GamePlayPenalties] PRIMARY KEY CLUSTERED 
(
	[GamePlayID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  UserDefinedFunction [dbo].[Fn_PlayerHistoicalData]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Matthew Veinot
-- Create date: November 23rd, 2021
-- Description:	Get player stats up until a date
-- =============================================
CREATE FUNCTION [dbo].[Fn_PlayerHistoicalData]
(	
	@PlayerID int,
	@DateEnd Date
)
RETURNS TABLE 
AS
RETURN 
(
	SELECT P.PlayerID, COUNT(*) AS 'PlayCount'
	FROM dbo.Players P
	INNER JOIN dbo.GamePlayPlayers GPP ON
	P.PlayerID = GPP.PlayerID
	INNER JOIN dbo.GamePlays GP ON
	GPP.GameID = GP.GameID
	INNER JOIN dbo.Games G ON
	GP.GameID = G.GameID
	LEFT JOIN dbo.GamePlayGoals GPG ON
	GP.GameID = GPG.GameID AND
	GP.PlayID = GPG.PlayID
	LEFT JOIN dbo.GamePlayPenalties GPPS ON
	GP.GameID = GPPS.GameID AND
	GP.PlayID = GPPS.PlayID
	WHERE P.PlayerID = @PlayerID AND G.Date <= @DateEnd
	GROUP BY P.PlayerID
)
GO
/****** Object:  View [dbo].[VI_GamePlayPenaltyOn]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VI_GamePlayPenaltyOn] AS
SELECT GP.GameID, GP.GamePlayID, GPP.PlayerID, P.PenaltySeverity, P.PenaltyMinutes
FROM dbo.GamePlays GP
INNER JOIN dbo.GamePlayPlayers GPP ON
GP.GamePlayID = GPP.GamePlayID
LEFT JOIN dbo.GamePlayPenalties P ON
GPP.GamePlayID = P.GamePlayID
WHERE GPP.PlayerType = 'PenaltyOn'
GO
/****** Object:  View [dbo].[VI_GamePlayPenaltyDrewBy]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[VI_GamePlayPenaltyDrewBy] AS
SELECT GP.GameID, GP.GamePlayID, GPP.PlayerID
FROM dbo.GamePlays GP
INNER JOIN dbo.GamePlayPlayers GPP ON
GP.GamePlayID = GPP.GamePlayID
WHERE GPP.PlayerType = 'DrewBy'
GO
/****** Object:  View [dbo].[VI_Game]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[VI_Game] AS
WITH Goals AS (
SELECT G.GameID, P.TeamID, SUM(1) AS 'Goals'
FROM dbo.Games G
INNER JOIN dbo.GamePlays GP ON
G.GameID = GP.GameID
INNER JOIN dbo.GamePlayPlayers GPP ON
GP.GamePlayID = GPP.GamePlayID
INNER JOIN dbo.GamePlayers P ON
G.GameID = P.GameID AND
GPP.PlayerID = P.PlayerID
WHERE GP.EventTypeID = 'GOAL' AND GPP.PlayerType = 'Scorer'
GROUP BY G.GameID, P.TeamID)

SELECT G.GameID, G.AwayTeamID, COALESCE(A.Goals, 0) AS 'AwayScore', G.HomeTeamID, COALESCE(H.Goals, 0) AS 'HomeScore', 
CASE WHEN MAX(GP.Period) > 4 THEN 'ShootOut' WHEN MAX(GP.Period) > 3 THEN 'Overtime' ELSE 'Normal' END AS 'GameEnded', 
CASE WHEN COALESCE(A.Goals, 0) > COALESCE(H.Goals, 0) THEN G.AwayTeamID ELSE G.HomeTeamID END AS 'WinningTeamID',
CASE WHEN COALESCE(A.Goals, 0) < COALESCE(H.Goals, 0) THEN G.AwayTeamID ELSE G.HomeTeamID END AS 'LosingTeamID',
GRA.RestDays AS 'AwayTeamRestDays', GRH.RestDays AS 'HomeTeamRestDays'
FROM dbo.Games G
INNER JOIN dbo.GamePlays GP ON 
G.GameID = GP.GameID
LEFT JOIN Goals A ON
G.GameID = A.GameID AND
G.AwayTeamID = A.TeamID
LEFT JOIN Goals H ON
G.GameID = H.GameID AND
G.HomeTeamID = H.TeamID
LEFT JOIN dbo.VI_GameTeamRestDays GRA ON
G.GameID = GRA.GameID AND
G.AwayTeamID = GRA.TeamID
LEFT JOIN dbo.VI_GameTeamRestDays GRH ON
G.GameID = GRH.GameID AND
G.AwayTeamID = GRH.TeamID

GROUP BY G.GameID, G.AwayTeamID, A.Goals, G.HomeTeamID, H.Goals, GRA.RestDays, GRH.RestDays
GO
/****** Object:  Table [dbo].[Conferences]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Conferences](
	[ConferenceID] [int] NOT NULL,
	[Name] [varchar](50) NOT NULL,
	[Active] [bit] NOT NULL,
 CONSTRAINT [PK_Conferences] PRIMARY KEY CLUSTERED 
(
	[ConferenceID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Divisions]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Divisions](
	[DivisionID] [int] NOT NULL,
	[Name] [varchar](50) NOT NULL,
	[ConferenceID] [int] NOT NULL,
	[Active] [bit] NOT NULL,
 CONSTRAINT [PK_Divisions] PRIMARY KEY CLUSTERED 
(
	[DivisionID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[EventType]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EventType](
	[EventTypeID] [varchar](20) NOT NULL,
	[Description] [varchar](50) NULL,
 CONSTRAINT [PK_EventType] PRIMARY KEY CLUSTERED 
(
	[EventTypeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Positions]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Positions](
	[PositionID] [varchar](3) NOT NULL,
	[Name] [varchar](50) NOT NULL,
 CONSTRAINT [PK_Position] PRIMARY KEY CLUSTERED 
(
	[PositionID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ProLineMarginTeams]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ProLineMarginTeams](
	[MatchID] [int] NOT NULL,
	[TeamID] [int] NOT NULL,
	[ByThreePlusOdds] [decimal](4, 2) NOT NULL,
	[ByTwoOdds] [decimal](4, 2) NOT NULL,
	[ByOneInRegOdds] [decimal](4, 2) NOT NULL,
	[OverTimeShootOutOdds] [decimal](4, 2) NOT NULL,
 CONSTRAINT [PK_ProLineMarginTeams] PRIMARY KEY CLUSTERED 
(
	[MatchID] ASC,
	[TeamID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Seasons]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Seasons](
	[SeasonID] [int] NOT NULL,
	[StartDate] [date] NOT NULL,
	[EndDate] [date] NULL,
 CONSTRAINT [PK_Seasons] PRIMARY KEY CLUSTERED 
(
	[SeasonID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TestCase]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TestCase](
	[TestCaseID] [int] NOT NULL,
	[Version] [int] NOT NULL,
	[GameBeginDate] [date] NOT NULL,
	[GameEndDate] [date] NOT NULL,
 CONSTRAINT [PK_TestCase] PRIMARY KEY CLUSTERED 
(
	[TestCaseID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TestCaseGameDetails]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TestCaseGameDetails](
	[TestCaseID] [int] NOT NULL,
	[GameID] [int] NOT NULL,
	[AwayGoals] [int] NOT NULL,
	[AwayGoalsEmpty] [int] NOT NULL,
	[AwayShots] [int] NOT NULL,
	[AwayAssists] [int] NOT NULL,
	[AwayFaceOffWon] [int] NOT NULL,
	[AwayFaceOffLost] [int] NOT NULL,
	[AwayMissedShots] [int] NOT NULL,
	[AwayBlocked] [int] NOT NULL,
	[AwayHitters] [int] NOT NULL,
	[AwayHittees] [int] NOT NULL,
	[AwayTakeAways] [int] NOT NULL,
	[AwayGiveAway] [int] NOT NULL,
	[AwayPenalties] [int] NOT NULL,
	[AwayPenaltyDrewBy] [int] NOT NULL,
	[AwayNumGames] [int] NOT NULL,
	[AwayDaysOfRest] [int] NOT NULL,
	[HomeGoals] [int] NOT NULL,
	[HomeGoalsEmpty] [int] NOT NULL,
	[HomeShots] [int] NOT NULL,
	[HomeAssists] [int] NOT NULL,
	[HomeFaceOffWon] [int] NOT NULL,
	[HomeFaceOffLost] [int] NOT NULL,
	[HomeMissedShots] [int] NOT NULL,
	[HomeBlocked] [int] NOT NULL,
	[HomeHitters] [int] NOT NULL,
	[HomeHittees] [int] NOT NULL,
	[HomeTakeAways] [int] NOT NULL,
	[HomeGiveAway] [int] NOT NULL,
	[HomePenalties] [int] NOT NULL,
	[HomePenaltyDrewBy] [int] NOT NULL,
	[HomeNumGames] [int] NOT NULL,
	[HomeDaysOfRest] [int] NOT NULL,
 CONSTRAINT [PK_TestCaseGameDetails] PRIMARY KEY CLUSTERED 
(
	[TestCaseID] ASC,
	[GameID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TestCaseGames]    Script Date: 2022-01-09 6:25:34 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TestCaseGames](
	[TestCaseID] [int] NOT NULL,
	[GameID] [int] NOT NULL,
	[HomeGoals] [int] NOT NULL,
	[AwayGoals] [int] NOT NULL,
 CONSTRAINT [PK_TestCaseGames] PRIMARY KEY CLUSTERED 
(
	[TestCaseID] ASC,
	[GameID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Divisions]  WITH CHECK ADD  CONSTRAINT [FK_Divisions_Conferences] FOREIGN KEY([ConferenceID])
REFERENCES [dbo].[Conferences] ([ConferenceID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[Divisions] CHECK CONSTRAINT [FK_Divisions_Conferences]
GO
ALTER TABLE [dbo].[GameGoalies]  WITH CHECK ADD  CONSTRAINT [FK_GameGoalies_Games] FOREIGN KEY([GameID])
REFERENCES [dbo].[Games] ([GameID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[GameGoalies] CHECK CONSTRAINT [FK_GameGoalies_Games]
GO
ALTER TABLE [dbo].[GamePlayers]  WITH CHECK ADD  CONSTRAINT [FK_GamePlayers_Games] FOREIGN KEY([GameID])
REFERENCES [dbo].[Games] ([GameID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[GamePlayers] CHECK CONSTRAINT [FK_GamePlayers_Games]
GO
ALTER TABLE [dbo].[GamePlayGoals]  WITH CHECK ADD  CONSTRAINT [FK_GamePlayGoals_GamePlays] FOREIGN KEY([GamePlayID])
REFERENCES [dbo].[GamePlays] ([GamePlayID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[GamePlayGoals] CHECK CONSTRAINT [FK_GamePlayGoals_GamePlays]
GO
ALTER TABLE [dbo].[GamePlayPenalties]  WITH CHECK ADD  CONSTRAINT [FK_GamePlayPenalties_GamePlays] FOREIGN KEY([GamePlayID])
REFERENCES [dbo].[GamePlays] ([GamePlayID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[GamePlayPenalties] CHECK CONSTRAINT [FK_GamePlayPenalties_GamePlays]
GO
ALTER TABLE [dbo].[GamePlayPlayers]  WITH CHECK ADD  CONSTRAINT [FK_GamePlayPlayers_GamePlays] FOREIGN KEY([GamePlayID])
REFERENCES [dbo].[GamePlays] ([GamePlayID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[GamePlayPlayers] CHECK CONSTRAINT [FK_GamePlayPlayers_GamePlays]
GO
ALTER TABLE [dbo].[GamePlayPlayers]  WITH CHECK ADD  CONSTRAINT [FK_GamePlayPlayers_Players] FOREIGN KEY([PlayerID])
REFERENCES [dbo].[Players] ([PlayerID])
GO
ALTER TABLE [dbo].[GamePlayPlayers] CHECK CONSTRAINT [FK_GamePlayPlayers_Players]
GO
ALTER TABLE [dbo].[GamePlays]  WITH CHECK ADD  CONSTRAINT [FK_GamePlays_EventType] FOREIGN KEY([EventTypeID])
REFERENCES [dbo].[EventType] ([EventTypeID])
GO
ALTER TABLE [dbo].[GamePlays] CHECK CONSTRAINT [FK_GamePlays_EventType]
GO
ALTER TABLE [dbo].[GamePlays]  WITH CHECK ADD  CONSTRAINT [FK_GamePlays_Games] FOREIGN KEY([GameID])
REFERENCES [dbo].[Games] ([GameID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[GamePlays] CHECK CONSTRAINT [FK_GamePlays_Games]
GO
ALTER TABLE [dbo].[Games]  WITH CHECK ADD  CONSTRAINT [FK_Games_Seasons] FOREIGN KEY([SeasonID])
REFERENCES [dbo].[Seasons] ([SeasonID])
GO
ALTER TABLE [dbo].[Games] CHECK CONSTRAINT [FK_Games_Seasons]
GO
ALTER TABLE [dbo].[Games]  WITH CHECK ADD  CONSTRAINT [FK_Games_Teams_Away] FOREIGN KEY([AwayTeamID])
REFERENCES [dbo].[Teams] ([TeamID])
GO
ALTER TABLE [dbo].[Games] CHECK CONSTRAINT [FK_Games_Teams_Away]
GO
ALTER TABLE [dbo].[Games]  WITH CHECK ADD  CONSTRAINT [FK_Games_Teams_Home] FOREIGN KEY([HomeTeamID])
REFERENCES [dbo].[Teams] ([TeamID])
GO
ALTER TABLE [dbo].[Games] CHECK CONSTRAINT [FK_Games_Teams_Home]
GO
ALTER TABLE [dbo].[PlayerInjuries]  WITH CHECK ADD  CONSTRAINT [FK_PlayerInjuries_PlayerInjuryStatuses] FOREIGN KEY([StatusID])
REFERENCES [dbo].[PlayerInjuryStatuses] ([StatusID])
GO
ALTER TABLE [dbo].[PlayerInjuries] CHECK CONSTRAINT [FK_PlayerInjuries_PlayerInjuryStatuses]
GO
ALTER TABLE [dbo].[PlayerInjuries]  WITH CHECK ADD  CONSTRAINT [FK_PlayerInjuries_Players] FOREIGN KEY([PlayerID])
REFERENCES [dbo].[Players] ([PlayerID])
GO
ALTER TABLE [dbo].[PlayerInjuries] CHECK CONSTRAINT [FK_PlayerInjuries_Players]
GO
ALTER TABLE [dbo].[Players]  WITH CHECK ADD  CONSTRAINT [FK_Players_Positions] FOREIGN KEY([PositionID])
REFERENCES [dbo].[Positions] ([PositionID])
GO
ALTER TABLE [dbo].[Players] CHECK CONSTRAINT [FK_Players_Positions]
GO
ALTER TABLE [dbo].[Players]  WITH CHECK ADD  CONSTRAINT [FK_Players_Teams] FOREIGN KEY([TeamID])
REFERENCES [dbo].[Teams] ([TeamID])
GO
ALTER TABLE [dbo].[Players] CHECK CONSTRAINT [FK_Players_Teams]
GO
ALTER TABLE [dbo].[ProLineMarginTeams]  WITH CHECK ADD  CONSTRAINT [FK_ProLineMarginTeams_ProLineOdds] FOREIGN KEY([MatchID])
REFERENCES [dbo].[ProLineOdds] ([MatchID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[ProLineMarginTeams] CHECK CONSTRAINT [FK_ProLineMarginTeams_ProLineOdds]
GO
ALTER TABLE [dbo].[ProLineMoneyLineTeams]  WITH CHECK ADD  CONSTRAINT [FK_ProLineMoneyLineTeams_ProLineOdds] FOREIGN KEY([MatchID])
REFERENCES [dbo].[ProLineOdds] ([MatchID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[ProLineMoneyLineTeams] CHECK CONSTRAINT [FK_ProLineMoneyLineTeams_ProLineOdds]
GO
ALTER TABLE [dbo].[ProLineOdds]  WITH CHECK ADD  CONSTRAINT [FK_ProLineOdds_Teams_Away] FOREIGN KEY([AwayTeamID])
REFERENCES [dbo].[Teams] ([TeamID])
GO
ALTER TABLE [dbo].[ProLineOdds] CHECK CONSTRAINT [FK_ProLineOdds_Teams_Away]
GO
ALTER TABLE [dbo].[ProLineOdds]  WITH CHECK ADD  CONSTRAINT [FK_ProLineOdds_Teams_Home] FOREIGN KEY([HomeTeamID])
REFERENCES [dbo].[Teams] ([TeamID])
GO
ALTER TABLE [dbo].[ProLineOdds] CHECK CONSTRAINT [FK_ProLineOdds_Teams_Home]
GO
ALTER TABLE [dbo].[ProLineSpreadsTeams]  WITH CHECK ADD  CONSTRAINT [FK_ProLineSpreadsTeams_ProLineOdds] FOREIGN KEY([MatchID])
REFERENCES [dbo].[ProLineOdds] ([MatchID])
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[ProLineSpreadsTeams] CHECK CONSTRAINT [FK_ProLineSpreadsTeams_ProLineOdds]
GO
ALTER TABLE [dbo].[Schedule]  WITH CHECK ADD  CONSTRAINT [FK_Schedule_Seasons] FOREIGN KEY([SeasonID])
REFERENCES [dbo].[Seasons] ([SeasonID])
GO
ALTER TABLE [dbo].[Schedule] CHECK CONSTRAINT [FK_Schedule_Seasons]
GO
ALTER TABLE [dbo].[Schedule]  WITH CHECK ADD  CONSTRAINT [FK_Schedule_Teams_Away] FOREIGN KEY([AwayTeamID])
REFERENCES [dbo].[Teams] ([TeamID])
GO
ALTER TABLE [dbo].[Schedule] CHECK CONSTRAINT [FK_Schedule_Teams_Away]
GO
ALTER TABLE [dbo].[Schedule]  WITH CHECK ADD  CONSTRAINT [FK_Schedule_Teams_Home] FOREIGN KEY([HomeTeamID])
REFERENCES [dbo].[Teams] ([TeamID])
GO
ALTER TABLE [dbo].[Schedule] CHECK CONSTRAINT [FK_Schedule_Teams_Home]
GO
ALTER TABLE [dbo].[Schedule]  WITH CHECK ADD  CONSTRAINT [FK_Schedule_Venues] FOREIGN KEY([VenueID])
REFERENCES [dbo].[Venues] ([VenueID])
GO
ALTER TABLE [dbo].[Schedule] CHECK CONSTRAINT [FK_Schedule_Venues]
GO
ALTER TABLE [dbo].[Teams]  WITH CHECK ADD  CONSTRAINT [FK_Players_Conferences] FOREIGN KEY([ConferenceID])
REFERENCES [dbo].[Conferences] ([ConferenceID])
GO
ALTER TABLE [dbo].[Teams] CHECK CONSTRAINT [FK_Players_Conferences]
GO
ALTER TABLE [dbo].[Teams]  WITH CHECK ADD  CONSTRAINT [FK_Teams_Venues] FOREIGN KEY([VenueID])
REFERENCES [dbo].[Venues] ([VenueID])
GO
ALTER TABLE [dbo].[Teams] CHECK CONSTRAINT [FK_Teams_Venues]
GO
ALTER TABLE [dbo].[TestCaseGameDetails]  WITH CHECK ADD  CONSTRAINT [FK_TestCaseGameDetails_TestCaseGames] FOREIGN KEY([TestCaseID], [GameID])
REFERENCES [dbo].[TestCaseGames] ([TestCaseID], [GameID])
GO
ALTER TABLE [dbo].[TestCaseGameDetails] CHECK CONSTRAINT [FK_TestCaseGameDetails_TestCaseGames]
GO
ALTER TABLE [dbo].[TestCaseGames]  WITH CHECK ADD  CONSTRAINT [FK_TestCaseGames_Games] FOREIGN KEY([GameID])
REFERENCES [dbo].[Games] ([GameID])
GO
ALTER TABLE [dbo].[TestCaseGames] CHECK CONSTRAINT [FK_TestCaseGames_Games]
GO
ALTER TABLE [dbo].[TestCaseGames]  WITH CHECK ADD  CONSTRAINT [FK_TestCaseGames_TestCase] FOREIGN KEY([TestCaseID])
REFERENCES [dbo].[TestCase] ([TestCaseID])
GO
ALTER TABLE [dbo].[TestCaseGames] CHECK CONSTRAINT [FK_TestCaseGames_TestCase]
GO
ALTER TABLE [dbo].[PlayerInjuries]  WITH CHECK ADD  CONSTRAINT [CK_PlayerInjuries_RecoveredDate] CHECK  (([RecoveredDate] IS NULL OR [RecoveredDate]>=[Date]))
GO
ALTER TABLE [dbo].[PlayerInjuries] CHECK CONSTRAINT [CK_PlayerInjuries_RecoveredDate]
GO
ALTER TABLE [dbo].[TestCase]  WITH CHECK ADD  CONSTRAINT [CK_TestCase_GameDate] CHECK  (([GameBeginDate]<=[GameEndDate]))
GO
ALTER TABLE [dbo].[TestCase] CHECK CONSTRAINT [CK_TestCase_GameDate]
GO
