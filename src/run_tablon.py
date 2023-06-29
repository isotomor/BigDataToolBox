from init_config import launcher

def run_ingest_campaint_mailchimp(project_data, **_):
    return None


if __name__ == "__main__":
    launcher(run_ingest_campaint_mailchimp, init_spark=True, list_days=True)
