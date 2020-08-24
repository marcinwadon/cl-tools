package cmd

import (
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "cl-tools",
		Short: "Constellation tools",
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
}