import csv
import secrets

import cloudscraper
import pendulum
import untangle
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

swing_param_dict = {
    # Standard swings
    "SHOTTIME_PARAMETER_STRING": "Time",
    "ATTEMPTINDEX_PARAMETER_STRING": "Shot",
    "LAUNCHSPEED_PARAMETER_STRING": "Ball (m/s)",
    "CLUBHEADSPEED_PARAMETER_STRING": "Club (m/s)",
    "SMASH_PARAMETER_STRING": "Smash",
    "CARRYDIST_PARAMETER_STRING": "Carry (m)",
    "TOTALDIST_PARAMETER_STRING": "Total (m)",
    "ROLLDIST_PARAMETER_STRING": "Roll (m)",
    "SPIN_PARAMETER_STRING": "Spin (rpm)",
    "SPIN_IS_ESTIMATE": "Spin estimated",
    "BACKSPIN_PARAMETER_STRING": "Backspin (rpm)",
    "SIDESPIN_PARAMETER_STRING": "Sidespin (rpm)",
    "HEIGHT_PARAMETER_STRING": "Height (m)",
    "FLIGHTTIME_PARAMETER_STRING": "Time (s)",
    "CLUBSTRIKEDIRVERT_PARAMETER_STRING": "AOA (*)",
    "SPINLOFT_PARAMETER_STRING": "Spin Loft (*)",
    "SPINAXIS_PARAMETER_STRING": "Spin Axis (*)",
    "LATERAL_PARAMETER_STRING": "Lateral (m)",
    "CURVEDIST_PARAMETER_STRING": "Curve (m)",
    "LAUNCHAZIM_PARAMETER_STRING": "Launch H(*)",
    "LAUNCHELEV_PARAMETER_STRING": "Launch V (*)",
    "SHOTCLASSIFICATION_PARAMETER_STRING": "Shape",
    "DETECTION_MODE_PARAMETER_STRING": "Mode",
    "IMPACTELEV_PARAMETER_STRING": "Landing Angle (*)",
    "LANDINGVELOCITY1_PARAMETER_STRING": "Landing Speed X (m/s)",
    "LANDINGVELOCITY2_PARAMETER_STRING": "Landing Speed Y (m/s)",
    "LANDINGVELOCITY3_PARAMETER_STRING": "Landing Speed Z (m/s)",
    "CLUBHEADSPEEDPOST_PARAMETER_STRING": "Club Post-Impact (m/s)",
    "RANGEBALL": "Range ball",

    # FS Skills params
    "SHOTSCORE_PARAMETER_STRING": "Score",
    "TARGETINDEX_PARAMETER_STRING": "Target Index",
    "DISTANCE_FROM_TARGETLINE": "Distance from Target Line (m)",
    "DISTANCE_FROM_PIN_CARRY": "Carry Distance from Pin (m)",
    "DISTANCE_FROM_PIN_TOTAL": "Total Distance from Pin (m)",
}

club_type_dict = {
    "1": "Driver",
    "2": "Iron",
    "3": "Wedge"
}

fs_skills_dict = {
    "Diameter": "Target Diameter (m)",
    "FairwayWidth": "Target Fairway Width (m)",
    "ForwardDist": "Target Forward Dist (m)",
    "LateralDist": "Target Lateral Dist (m)",
    "ZoneSize1": "Target Zone 1 Size (m)",
    "ZoneSize2": "Target Zone 2 Size (m)",
    "ZoneSize3": "Target Zone 3 Size (m)",
}

default_output_headers = [
    "Session ID",
    "Session Name",
    "Session Created"
    "Swing ID",
    "Club Name",
    "Shot",
    "Ball (m/s)",
    "Club (m/s)",
    "Smash",
    "Carry (m)",
    "Total (m)",
    "Backspin (rpm)",
    "Sidespin (rpm)",
    "Spin estimated",
    "Height (m)",
    "Time (s)",
    "AOA (*)",
    "Spin Loft (*)",
    "Lateral (m)",
    "Curve (m)",
    "Launch H(*)",
    "Launch V (*)",
    "Landing Angle (*)",
]

fs_golf_headers = default_output_headers + [
    "Spin Axis (*)",
    "Shape",
    "Range ball",
    "Mode",
]
fs_skills_headers = default_output_headers + [
    "Template ID",
    "Score",
    "Target ID",
    "Target Forward Dist (m)",
    "Target Diameter (m)",
    "Distance from Target Line (m)",
    "Carry Distance from Pin (m)",
    "Total Distance from Pin (m)"
]

BEGINNING_OF_TIME = pendulum.parse("2011-12-31")


def login(username, password):
    """
    Create a scraper and login to myflightscope.com

    Returns a logged in scraper
    """

    # log in
    scraper = cloudscraper.create_scraper(browser="chrome")

    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "PUT", "DELETE", "POST"],
        backoff_factor=0,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    scraper.mount("https://", adapter)
    scraper.mount("http://", adapter)

    payload = {
        "rememberme": "forever",
        "redirect_to": "/",
        "pwd": password,
        "log": username,
        "wp-submit": "Login",
    }
    resp = scraper.request(
        "POST", "https://myflightscope.com/wp-login.php", data=payload
    )
    if resp.ok:
        print("Logged in OK")
        return scraper
    else:
        raise Exception("Failed to login")


def get_sessions(
    scraper,
    player_id,
    start_date=None,
    end_date=None,
    app="ALL",
):
    """
    Downloads sessions from a logged in scraper.Outputs as a list of untangle objects

    Arguments
    - scraper [Cloudscraper] - the logged in scraper
    - player_id [String] - the player_id to filter for
    - start_date [Datetime] - the first date to start pulling sessions from. Defaults
        to all data.
    - end_date [Datetime] - the last date to pull sessions from. Defaults to today.
    - app [String] - the Flightscope app (FS_GOLF, FS_SKILLS, etc) to pull data
        for. Defaults to "All"

    Returns a list of [untangle] session objects
    """
    start_date = BEGINNING_OF_TIME if start_date is None else start_date
    end_date = pendulum.now() if end_date is None else end_date

    # Loop through FS Golf sessions
    min_ix = 0
    max_ix = 1
    incr = 250
    sessions = []

    while min_ix < max_ix:
        print(
            f"Getting data starting from {min_ix} to {max_ix} in increments of {incr}"
        )
        payload = {
            "playerID": player_id,
            "filterApp": app,
            "filterStartDate": start_date.format("YYYY-MM-DD"),
            "filterEndDate": end_date.format("YYYY-MM-DD"),
            "lookingFor": "",
            "startIndex": min_ix,
            "count": incr,
            "method": "listSessionsWithScoreForPlayerAndFilter",
        }
        resp = scraper.request(
            "POST", "https://myflightscope.com/SoapFrame/index.php", data=payload
        )
        if resp.ok:
            print("Successfully pulled data")
        else:
            raise Exception("Failed to retrieve sessions")

        list_of_sessions = untangle.parse(resp.text)
        sessions.extend(list(list_of_sessions.Sessions.Session))

        min_ix = int(list_of_sessions.Sessions["recordCount"])
        if min_ix == incr:
            max_ix += incr
    return sessions


# for session in list_of_sessions.Sessions.Session:
def get_swings(
    scraper,
    session,
    player_id,
):
    """
    Download swings from a single session

    Returns a list with:
        0: session meta as a dict
        1: an list of swings as a dict
    """
    # Loop through FS Golf pages
    print(f"Downloading session {session.sessionID.cdata}")
    session_payload = {
        "playerID": player_id,
        "sessionID": session.sessionID.cdata,
        "method": "GetSession",
    }
    session_resp = scraper.request(
        "POST", "https://myflightscope.com/SoapFrame/index.php", data=session_payload
    )

    if session_resp.ok:
        print(f"Successfully downloaded session {session.sessionID.cdata}")
    else:
        raise Exception(f"Session {session.ID} failed to load")

    session_data = untangle.parse(session_resp.text).Sessions.Session
    session_meta = {
        "Session ID": session_data.ID.cdata,
        "Session Name": session_data.DisplayName.cdata,
        "Session Created": session_data.CreateDate.cdata,
        "Session Ended": session_data.EndDate.cdata,
        "Session Creator ID": session_data.CreatorID.cdata,
        "Session Type": session_data.SessionType.cdata,
        "Session Type ID": session_data.SessionTypeID.cdata,
    }

    # Get app specific templates
    app_dict = {}
    if "SkillsAssessmentTemplate" in dir(session_data):
        app_dict = extract_skills_dict(session_data)
        session_meta.update(app_dict)

    # Get Players and Clubs
    player_dict = {}
    club_dict = {}
    ball_dict = {}
    for p in session_data.PlayersForSession.PlayerForSession:
        player_dict[p.Player.ID.cdata] = p.Player.DisplayName.cdata
        for club in p.ClubsForPlayerForSession.ClubForPlayerForSession:
            club_dict[club.Club.ID.cdata] = club.Club.DisplayName.cdata
        if len(p.BallsForPlayerForSession) > 0:
            for ball in p.BallsForPlayerForSession.BallForPlayerForSession:
                ball_dict[ball.Ball.ID.cdata] = ball.Ball.Displayname.cdata

    # Iterate over swings
    swings = []
    print(f"Iterating over {len(session_data.GolfSwings.GolfSwing)} swings in session")
    for swing in session_data.GolfSwings.GolfSwing:
        swing_data = extract_swing(swing)
        # Map IDs
        if len(player_dict) > 0 and swing_data.get("Player ID"):
            swing_data['Player Name'] = player_dict.get(swing_data['Player ID'], "-")
        if len(club_type_dict) > 0 and swing_data.get("Club ID"):
            swing_data['Club Name'] = club_dict.get(swing_data['Club ID'], "-")
        if len(ball_dict) > 0 and swing_data.get("Ball ID"):
            swing_data['Ball Name'] = ball_dict.get(swing_data['Ball ID'], "-")
        if len(app_dict) > 0 and swing_data.get("Target Index"):
            swing_data.update(app_dict["Targets"].get(swing_data["Target Index"], {}))
        swings.append(swing_data)

    return [session_meta, swings]


# Write data to CSV
def write_to_csv(sessions_swings, headers, file):
    """
    Write the session-swings array to a specific location with given headers
    """

    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for session, swings in sessions_swings:
            for swing in swings:
                row = []
                for col in headers:
                    if col in session.keys():
                        row.append(session.get(col, "-"))
                    elif col in swing.keys():
                        row.append(swing.get(col, "-"))
                    else:
                        row.append("-")
                writer.writerow(row)


def extract_swing(
    swing
):
    """
    Pull known swing data out of a Swing XML

    Returns a dict of swing data
    """
    known_keys = swing_param_dict.keys()
    swing_data = {
        "Player ID": swing.PlayerID.cdata,
        "Club ID": swing.ClubID.cdata,
        "Ball ID": swing.BallID.cdata,
        "Swing ID": swing.GolfSwingID.cdata,
        "Swing Index": swing.SwingIndex.cdata,
        "Club Type ID": swing.clubTypeID.cdata,
    }
    try:
        params = [
            swing.GolfSwingParameters.GolfSwingParameter,
            swing.Result.ResultParameters.ResultParameter,
        ]
        for param_set in params:
            for param in param_set:
                if param.ParameterName.cdata in known_keys:
                    key = swing_param_dict[param.ParameterName.cdata]
                    swing_data[key] = param.ParameterValue.cdata
        print(f"Successfully added swing {swing.SwingIndex.cdata}")
    except AttributeError:
        print("Skipping swing due to malformed data")
    return swing_data


def extract_skills_dict(session_data):
    """
    Extract template and target data from FS Skills sessions
    """
    skills_dict = {}
    known_keys = fs_skills_dict.keys()

    template = session_data.SkillsAssessmentTemplate
    skills_dict.update({
        "Template ID": template.ID.cdata,
        "Template Name": template.DisplayName.cdata,
        "Template Created": template.CreateDate.cdata,
        "Template User Defined": template.UserDef.cdata,
        "Targets": {}
    })
    for target in template.SkillsAssessmentTargets.SkillsAssessmentTarget:
        target_meta = {
            "Target Index": target.TargetIndex.cdata,
            "Target ID": target.ID.cdata,
            "Target Attempts": target.AttemptsAtTarget.cdata,
            "Target Shape ID": target.TargetShapeID.cdata,
        }
        for param in (
            target.SkillsAssessmentTargetParameters.SkillsAssessmentTargetParameter
        ):
            if param.ParameterName.cdata in known_keys:
                key = fs_skills_dict[param.ParameterName.cdata]
                target_meta[key] = param.ParameterValue.cdata
        skills_dict["Targets"][target_meta["Target Index"]] = target_meta
    return skills_dict


def flightscope_to_csv(
    login_secrets=None,
    start_date=None,
    end_date=None,
    output_file=None,
    headers=None,
    app="ALL"
):
    """
    Download your data from MyFlightscope.com and output to a CSV

    Arguments
    - login_secrets [Dict] - The secrets you'd like to use for login. Must contain
        3 keys: username, password, and player_id
    - start_date [Datetime] - the first date to start pulling sessions from. Defaults
        to all data.
    - end_date [Datetime] - the last date to pull sessions from. Defaults to today.
    - output_file [String] - the file to write the CSV to
    - headers [List] - a list of data columns to write to the CSV
    - app [String] - the Flightscope app (FS_GOLF, SKILLS, etc) to pull data
        for. Defaults to "All"
    """
    login_secrets = secrets.login if login_secrets is None else login_secrets
    if output_file is None:
        output_file = f"output/{app}-{pendulum.now().format('YYYY-MM-DD_HH-mm-ss')}.csv"
    headers = default_output_headers if headers is None else headers

    scraper = login(login_secrets["username"], login_secrets["password"])
    sessions = get_sessions(
        scraper,
        login_secrets["player_id"],
        start_date,
        end_date,
        app
    )

    sessions_swings = []
    for s in sessions:
        session_swings = get_swings(scraper, s, login_secrets["player_id"])
        sessions_swings.append(session_swings)

    write_to_csv(sessions_swings, headers, output_file)
