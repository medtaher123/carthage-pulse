"""Main TUI launcher for Carthage Pulse services"""

import sys
import questionary


def run_ingestion():
    from ingestion.main import main

    main()


def run_processing():
    from processing.main import main

    main()


def run_storage():
    from storage.main import main

    main()


def run_get_active_streams():
    from speed.get_active_streams import main

    main()


def run_terminate_all_spark():
    from speed.terminate_all_spark import main

    main()


def run_speed_save_raw():
    from speed.save_raw import main

    main()


def run_speed_save_enriched():
    from speed.save_enriched import main

    main()


def run_speed_trending_topics():
    from speed.trending_topics import main

    main()


def run_speed_trending_words():
    from speed.trending_words import main

    main()


def run_speed_menu():
    speed_services = [
        questionary.Separator(),
        questionary.Choice(
            title="[1] Save Raw Stream",
            value=run_speed_save_raw,
        ),
        questionary.Choice(
            title="[2] Save Enriched Stream",
            value=run_speed_save_enriched,
        ),
        questionary.Choice(
            title="[3] Trending Topics",
            value=run_speed_trending_topics,
        ),
        questionary.Choice(
            title="[4] Trending Words",
            value=run_speed_trending_words,
        ),
        questionary.Separator(),
        questionary.Choice(
            title="[0] Back",
            value="back",
        ),
    ]

    while True:
        choice = questionary.select(
            "Speed Layer - Select a job:",
            choices=speed_services,
        ).ask()

        if choice in (None, "back"):
            return  # go back to main menu

        print("\nLaunching speed job...\n")
        try:
            choice()
        except KeyboardInterrupt:
            print("\n\nJob stopped.")
        except Exception as e:
            print(f"\nJob exited with error: {e}")

        print("\n" + "=" * 50 + "\n")


def main():
    services = [
        questionary.Separator(),
        questionary.Choice(
            title="[1] Ingestion - Stream posts from Reddit to Kafka",
            value=run_ingestion,
        ),
        questionary.Choice(
            title="[2] Processing - Enrich Kafka events with LLM analysis",
            value=run_processing,
        ),
        questionary.Choice(
            title="[3] Storage - Persist enriched events to storage",
            value=run_storage,
        ),
        questionary.Choice(
            title="[4] Speed - Run Spark Streaming jobs for real-time analytics",
            value=run_speed_menu,
        ),
        questionary.Choice(
            title="[5] Batch - Run Spark Batch jobs for historical analysis",
            value=lambda: print("Spark Batch service is not implemented yet."),
        ),
        questionary.Separator(),
        questionary.Choice(
            title="[6] Active - View a list of all active background Spark jobs",
            value=run_get_active_streams,
        ),
        questionary.Choice(
            title="[7] Terminate - Stop all background Spark jobs",
            value=run_terminate_all_spark,
        ),
        questionary.Separator(),
    ]

    while True:
        choice = questionary.select(
            "Select a service to run:",
            choices=services,
            style=questionary.Style(
                [
                    ("question", "fg:#87afff bold"),
                    ("answer", "fg:#ffffff bold"),
                    ("pointer", "fg:#87afff bold"),
                    ("highlighted", "fg:#87afff"),
                    ("selected", "fg:#ffffff"),
                ]
            ),
        ).ask()

        if choice is None:
            print("Goodbye!")
            sys.exit(0)

        print(f"\nLaunching service...\n")
        try:
            choice()
        except KeyboardInterrupt:
            print("\n\nService stopped.")
        except Exception as e:
            print(f"\nService exited with error: {e}")

        print("\n" + "=" * 50 + "\n")


if __name__ == "__main__":
    main()
