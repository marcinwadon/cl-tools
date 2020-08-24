package aws

import (
	"cl-tools/reporter"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"fmt"
	"os"
	"sync"
)

type AWSS3 interface {
	CheckHeights(bucket string, start int64, end int64, workers int)
}

type awsS3 struct {
	s3 *s3.S3
	reporter reporter.Reporter
}

func InitAWSS3(region string, rep reporter.Reporter) AWSS3 {
	s3Client := createSession(region)

	return &awsS3{
		s3: s3Client,
		reporter: rep,
	}
}

func createSession(region string) *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		exitErrorf("Unable to create a new S3 session, %v", err)
	}

	return s3.New(sess)
}

func (a *awsS3) worker(jobChan <-chan int64, bucket string, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobChan {
		a.checkHeight(bucket, job)
	}
}

func (a *awsS3) CheckHeights(bucket string, start int64, end int64, workers int) {
	var wg sync.WaitGroup

	jobChan := make(chan int64)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go a.worker(jobChan, bucket, &wg)
	}

	for i := start; i <= end; i = i + 2 {
		jobChan <- i
	}

	close(jobChan)
	wg.Wait()

	fmt.Printf("\n ** Checked heights: %d (%d - %d)\n", (end - start) / 2 + 1, start, end)
}

func (a *awsS3) checkHeight(bucket string, height int64) (int64, bool) {
	fmt.Printf("Check %d\n", height)

	result, err := a.s3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("snapshots/" + fmt.Sprintf("%d", height) + "-"),
	})
	if err != nil {
		exitErrorf("Unable to check height, %v", err)
	}

	if len(result.Contents) == 2 {
		return height, true
	}

	a.reporter.ReportS3Gap(height)
	return height, false
}


func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}