from fill_schedules import fill_schedules
from np_data_fetch import parse_nord_pool_data
from remove_used_schedules import refresh_schedules
from search_for_available_schedules import search_for_available_schedules
from search_for_used_schedules import search_for_outdated_schedules
from send_notification import send_schedule_update_notification_via_email


def run_cleaning_of_schedules():
    search_for_outdated_schedules()
    refresh_schedules()

def fill_leftovers():
    run_cleaning_of_schedules()
    with open("./new_schedules.txt", "r") as file:
        schedules = file.readlines()
    if len(schedules) != 0:
        fill_schedules()
    else:
        print("There are no leftovers.")

def main():
    parsed_data = parse_nord_pool_data(country_code='LV')
    if parsed_data is not None:
        run_cleaning_of_schedules()
        search_for_available_schedules()
        fill_schedules()
        send_schedule_update_notification_via_email(parsed_data=parsed_data, country=list(parsed_data.keys())[0])
    else:
        print("Nothing to update.")


if __name__ == "__main__":
    main()