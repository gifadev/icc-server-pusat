import subprocess
import logging
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional

def get_last_location() -> List[Dict[str, Optional[str]]]:
    adb_command = "adb shell dumpsys location | grep 'last location'"
    try:
        output = subprocess.check_output(adb_command, shell=True).decode("utf-8")
    except subprocess.SubprocessError as e:
        logging.error(f"Error getting last location: {e}")
        return []

    locations = []
    current_time = datetime.now()

    for line in output.split("\n"):
        if "last location" in line:
            et_match = re.search(r"et=\+\d{1,}d\d{1,}h\d{1,}m\d{1,}s\d{1,}ms", line)
            datetime_str = None

            if et_match:
                et_str = et_match.group(0)
                days = int(re.search(r"\d+(?=d)", et_str).group(0))
                hours = int(re.search(r"\d+(?=h)", et_str).group(0))
                minutes = int(re.search(r"\d+(?=m)", et_str).group(0))
                seconds = int(re.search(r"\d+(?=s)", et_str).group(0))
                milliseconds = int(re.search(r"\d+(?=ms)", et_str).group(0))

                elapsed_time = timedelta(
                    days=days,
                    hours=hours,
                    minutes=minutes,
                    seconds=seconds,
                    milliseconds=milliseconds,
                )
                location_time = current_time - elapsed_time
                datetime_str = location_time.strftime("%Y-%m-%d %H:%M:%S")

            coordinates = re.findall(r"[-+]?\d*\.\d+|\d+", line)
            if len(coordinates) >= 2:
                location = {
                    "latitude": float(coordinates[0]),
                    "longitude": float(coordinates[1]),
                    "datetime": datetime_str,
                }
                if "fused" in line:
                    location["provider"] = "fused"
                elif "network" in line:
                    location["provider"] = "network"
                elif "gps" in line:
                    location["provider"] = "gps"
                locations.append(location)
    return locations


#if '__name__' == __main__:
print(get_last_location()) 
