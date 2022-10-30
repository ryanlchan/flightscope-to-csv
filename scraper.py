import csv
import secrets
import cloudscraper
import pendulum
import untangle
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# load credentials
username = secrets.USERNAME
password = secrets.PASSWORD
playerid = secrets.PLAYER_ID

# log in
scraper = cloudscraper.create_scraper()
headers = {
    "sec-ch-ua": ('" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"'),
    "sec-ch-ua-mobile": "?0",
}

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
resp = scraper.request("POST", "https://myflightscope.com/wp-login.php", data=payload)
if resp.ok:
    print("Logged in OK")
else:
    raise Exception("Failed to login")


# Loop through FS Golf sessions
min_ix = 0
max_ix = 1
incr = 250
output = []
output_headers = [
    "sessionId",
    "Session",
    "Time",
    "Club",
    "Shot",
    "Ball (m/s)",
    "Club (m/s)",
    "Smash",
    "Carry (m)",
    "Total (m)",
    "Spin (rpm)",
    "Sidespin (rpm)",
    "Spin estimated",
    "Height (m)",
    "Time (s)",
    "AOA (*)",
    "Spin Loft (*)",
    "Spin Axis (*)",
    "Lateral (m)",
    "Curve (m)",
    "Launch H(*)",
    "Launch V (*)",
    "Shape",
    "Mode",
]
output.append(output_headers)
swing_param_dict = {
    "IMPACTELEV_PARAMETER_STRING": "Unknown",
    "CLUBHEADSPEEDPOST_PARAMETER_STRING": "Unknown",
    "SHOTTIME_PARAMETER_STRING": "Time",
    "LAUNCHSPEED_PARAMETER_STRING": "Ball (m/s)",
    "CLUBHEADSPEED_PARAMETER_STRING": "Club (m/s)",
    "SMASH_PARAMETER_STRING": "Smash",
    "CARRYDIST_PARAMETER_STRING": "Carry (m)",
    "TOTALDIST_PARAMETER_STRING": "Total (m)",
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
}

while min_ix < max_ix:
    print(f"Getting data starting from {min_ix} to {max_ix} in increments of {incr}")
    payload = {
        "playerID": playerid,
        "filterApp": "FS_GOLF",
        "filterStartDate": "2011-12-31",
        "filterEndDate": pendulum.now().format("YYYY-MM-DD"),
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
    # Loop through FS Golf pages
    for session in list_of_sessions.Sessions.Session:
        print(f"Downloading session {session.sessionID.cdata}")
        session_payload = {
            "playerID": playerid,
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

        # Get Players and Clubs
        session_data = untangle.parse(session_resp.text).Sessions.Session
        players = session_data.PlayersForSession.PlayerForSession
        club_dict = {}
        for club in players.ClubsForPlayerForSession.ClubForPlayerForSession:
            club_dict[club.Club.ID.cdata] = club.Club.DisplayName.cdata

        # Iterate over swings
        print(f"Iterating over {len(session_data.GolfSwings.GolfSwing)} swings in session")
        for swing in session_data.GolfSwings.GolfSwing:
            try:
                swing_data = {}
                for param in swing.GolfSwingParameters.GolfSwingParameter:
                    if param.ParameterName.cdata in swing_param_dict.keys():
                        pk = swing_param_dict[param.ParameterName.cdata]
                        swing_data[pk] = param.ParameterValue.cdata
                for param in swing.Result.ResultParameters.ResultParameter:
                    if param.ParameterName.cdata in swing_param_dict.keys():
                        pk = swing_param_dict[param.ParameterName.cdata]
                        swing_data[pk] = param.ParameterValue.cdata
                output.append(
                    [
                        session_data.ID.cdata,
                        session_data.CreateDate.cdata,
                        swing_data.get("Time","-"),
                        club_dict[swing.ClubID.cdata],
                        swing.SwingIndex.cdata,
                        swing_data.get("Ball (m/s)","-"),
                        swing_data.get("Club (m/s)","-"),
                        swing_data.get("Smash","-"),
                        swing_data.get("Carry (m)","-"),
                        swing_data.get("Total (m)","-"),
                        swing_data.get("Spin (rpm)","-"),
                        swing_data.get("Sidespin (rpm)","-"),
                        swing_data.get("Spin estimated","-"),
                        swing_data.get("Height (m)","-"),
                        swing_data.get("Time (s)","-"),
                        swing_data.get("AOA (*)","-"),
                        swing_data.get("Spin Loft (*)","-"),
                        swing_data.get("Spin Axis (*)","-"),
                        swing_data.get("Lateral (m)","-"),
                        swing_data.get("Curve (m)","-"),
                        swing_data.get("Launch H(*)","-"),
                        swing_data.get("Launch V (*)","-"),
                        swing_data.get("Shape","-"),
                        swing_data.get("Mode","-"),
                    ]
                )
                print(f"Successfully added swing {swing.SwingIndex.cdata}")
            except AttributeError:
                print(f"Skipping swing due to malformed data")
    min_ix = int(list_of_sessions.Sessions["recordCount"])
    if min_ix == incr:
        max_ix += incr

# Write data to CSV
with open(f"output/{pendulum.now().to_atom_string()}.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(output)
