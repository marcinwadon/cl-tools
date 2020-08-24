package cmd

import (
	"cl-tools/aws"
	reporter "cl-tools/reporter"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(checkGapsS3Cmd)
	checkGapsS3Cmd.Flags().StringP("bucket", "b", "", "S3 bucket name (required!)")
	checkGapsS3Cmd.Flags().String("region", "us-west-1", "S3 bucket region")
	checkGapsS3Cmd.Flags().Int64P("start", "s", 2, "Starting height")
	checkGapsS3Cmd.Flags().Int64P("end", "e", 0, "Ending height")
	checkGapsS3Cmd.Flags().IntP("workers", "w", 20, "Amount of parallel workers")

	checkGapsS3Cmd.MarkFlagRequired("bucket")
}

func checkGapsInS3(cmd *cobra.Command, args []string) {
	bucket, _ := cmd.Flags().GetString("bucket")
	region, _ := cmd.Flags().GetString("region")
	start, _ := cmd.Flags().GetInt64("start")
	end, _ := cmd.Flags().GetInt64("end")
	workers, _ := cmd.Flags().GetInt("workers")

	rep := reporter.NewReporter()
	awsS3 := aws.InitAWSS3(region, rep)
	awsS3.CheckHeights(bucket, start, end, workers)

}

var checkGapsS3Cmd = &cobra.Command{
	Use:   "check-gaps-s3",
	Short: "Check in s3 - all gaps or between given heights if specified",
	Run: func(cmd *cobra.Command, args []string) {
		checkGapsInS3(cmd, args)
	},
}
