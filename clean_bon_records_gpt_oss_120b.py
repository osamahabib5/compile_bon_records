import clean_bon_records_qwen as base


class GPTOSSUsageTracker(base.UsageTracker):
    def print_session_summary(self):
        cost = ((self.session_in_tokens / 1_000_000) * base.INPUT_COST_1M) + (
            (self.session_out_tokens / 1_000_000) * base.OUTPUT_COST_1M
        )
        print("\n" + "=" * 45)
        print("GPT OSS 120B CLEANING SUMMARY")
        print(f"Total Tokens:   {self.session_in_tokens + self.session_out_tokens:,}")
        print(f" - Input:       {self.session_in_tokens:,}")
        print(f" - Output:      {self.session_out_tokens:,}")
        print(f"Total Cost:     ${cost:.4f}")
        print("=" * 45)


base.MODEL_NAME = "openai/gpt-oss-120b"

# GPT OSS 120B limits: 8K TPM / 30 RPM / 200K daily
base.TPM_LIMIT = 7800
base.RPM_LIMIT = 28
base.DAILY_TOKEN_LIMIT = 195000
base.DAILY_REQUEST_LIMIT = 980
base.REQUEST_DELAY = 2.2

base.OUTPUT_FILE = "Validated_Records_files.xlsx"
base.USAGE_LOG_FILE = "groq_gpt_oss_120b_usage_log.json"

# Update if you want exact pricing in the console summary.
base.INPUT_COST_1M = 0.0
base.OUTPUT_COST_1M = 0.0

# Recreate the tracker after swapping the usage log file and limits.
base.tracker = GPTOSSUsageTracker(base.USAGE_LOG_FILE)


if __name__ == "__main__":
    base.main()
