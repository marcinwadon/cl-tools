package cmd

import (
	"cl-tools/es"
	"cl-tools/reporter"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(checkGapsESCmd)
	checkGapsESCmd.Flags().StringP("url", "u", "", "ES url (required!)")
	checkGapsESCmd.Flags().Int64P("start", "s", 2, "Starting height")
	checkGapsESCmd.Flags().Int64P("end", "e", 0, "Ending height")
	checkGapsESCmd.Flags().IntP("workers", "w", 20, "Amount of parallel workers")

	checkGapsESCmd.MarkFlagRequired("url")
}

func checkGapsInES(cmd *cobra.Command, args []string) {
	url, _ := cmd.Flags().GetString("url")
	start, _ := cmd.Flags().GetInt64("start")
	end, _ := cmd.Flags().GetInt64("end")
	workers, _ := cmd.Flags().GetInt("workers")

	rep := reporter.NewReporter()

	e := es.InitES(url, rep)
	e.CheckHeights(start, end, workers)

}

var checkGapsESCmd = &cobra.Command{
	Use:   "check-gaps-es",
	Short: "Check in ES - all gaps or between given heights if specified",
	Run: func(cmd *cobra.Command, args []string) {
		checkGapsInES(cmd, args)
	},
}
