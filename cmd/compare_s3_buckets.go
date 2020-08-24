package cmd

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strconv"
	"strings"
	"github.com/juliangruber/go-intersect"
)

func init() {
	rootCmd.AddCommand(compareS3BucketsCmd)
}

func compareS3Buckets(cmd *cobra.Command, args []string) {
	missingHeights := make(map[int][]int, 0)

	for i, bucket := range args {
		bucketMissingHeights := make([]int, 0, 0)

		f, err := os.Open(bucket)
		if err != nil {
			exitErrorf("Unable to open file: %v", err)
		}

		defer f.Close()

		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
			line := scanner.Text()
			replaced := strings.ReplaceAll(line, "Height: ", "")
			height, _ := strconv.Atoi(replaced)
			bucketMissingHeights = append(bucketMissingHeights, height)
		}

		missingHeights[i] = bucketMissingHeights
	}

	inter := intersect.Simple(missingHeights[0], missingHeights[1])
	i := 2

	if len(missingHeights) > 2 {
		inter = intersect.Simple(inter, missingHeights[i])
		i++
	}

	fmt.Printf("Missing heights:\n")
	fmt.Println(inter)
}

var compareS3BucketsCmd = &cobra.Command{
	Use:   "compare-s3-buckets <node1-log-path> <node2-log-path> ...",
	Args: cobra.MinimumNArgs(2),
	Short: "Compare bucket logs to intersect missing snapshots",
	Run: func(cmd *cobra.Command, args []string) {
		compareS3Buckets(cmd, args)
	},
}



func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}