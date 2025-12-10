package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"google.golang.org/grpc"
	// The latest profiler sends the data gzip encoded.
	_ "google.golang.org/grpc/encoding/gzip"
)

func newProfilesServer(cfg Config) *profilesServer {
	return &profilesServer{
		config: cfg,
	}
}

type Config struct {
	ExportResourceAttributes         bool
	ExportProfileAttributes          bool
	ExportSampleAttributes           bool
	ExportStackFrames                bool
	ExportStackFrameTypes            []string
	IgnoreProfilesWithoutContainerID bool
	FilterSampleTypes                []string
}

type profilesServer struct {
	pprofileotlp.UnimplementedGRPCServer
	config Config
}

func (f *profilesServer) Export(ctx context.Context, request pprofileotlp.ExportRequest) (pprofileotlp.ExportResponse, error) {
	dumpProfile(f.config, request.Profiles())

	return pprofileotlp.NewExportResponse(), nil
}

func dumpProfile(config Config, pd pprofile.Profiles) {
	mappingTable := pd.Dictionary().MappingTable()
	locationTable := pd.Dictionary().LocationTable()
	attributeTable := pd.Dictionary().AttributeTable()
	functionTable := pd.Dictionary().FunctionTable()
	stringTable := pd.Dictionary().StringTable()
	rps := pd.ResourceProfiles()
	for i := 0; i < rps.Len(); i++ {
		rp := rps.At(i)

		if config.IgnoreProfilesWithoutContainerID {
			containerID, ok := rp.Resource().Attributes().Get("container.id")
			if !ok || containerID.AsString() == "" {
				fmt.Println("--------------- New Resource Profile --------------")
				fmt.Println("              SKIPPED (no container.id)")
				fmt.Printf("-------------- End Resource Profile ---------------\n\n")
				continue
			}
		}

		fmt.Println("--------------- New Resource Profile --------------")
		if config.ExportResourceAttributes {
			if rp.Resource().Attributes().Len() > 0 {
				rp.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
					fmt.Printf("  %s: %v\n", k, v.AsString())
					return true
				})
			}
		}

		sps := rp.ScopeProfiles()
		for j := 0; j < sps.Len(); j++ {
			pcs := sps.At(j).Profiles()
			for k := 0; k < pcs.Len(); k++ {
				profile := pcs.At(k)
				sampleType := stringTable.At(int(profile.SampleType().TypeStrindex()))

				if len(config.FilterSampleTypes) > 0 && !slices.Contains(config.FilterSampleTypes, sampleType) {
					continue
				}

				fmt.Println("------------------- New Profile -------------------")
				fmt.Printf("  ProfileID: %x\n", [16]byte(profile.ProfileID()))
				fmt.Printf("  Time: %v\n", profile.Time().AsTime())
				fmt.Printf("  Duration: %v\n", time.Duration(profile.DurationNano()*uint64(time.Nanosecond)))
				fmt.Printf("  PeriodType: [%v, %v]\n",
					stringTable.At(int(profile.PeriodType().TypeStrindex())),
					stringTable.At(int(profile.PeriodType().UnitStrindex())))

				fmt.Printf("  Period: %v\n", profile.Period())
				fmt.Printf("  Dropped attributes count: %d\n", profile.DroppedAttributesCount())
				fmt.Printf("  SampleType: %s\n", sampleType)

				profileAttrs := profile.AttributeIndices()
				if profileAttrs.Len() > 0 {
					for n := 0; n < profileAttrs.Len(); n++ {
						attr := attributeTable.At(int(profileAttrs.At(n)))
						fmt.Printf("  %s: %s\n", stringTable.At(int(attr.KeyStrindex())), attr.Value().AsString())
					}
					fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
				}

				samples := profile.Samples()

				for l := 0; l < samples.Len(); l++ {
					sample := samples.At(l)

					fmt.Println("------------------- New Sample --------------------")

					for t := 0; t < sample.TimestampsUnixNano().Len(); t++ {
						sampleTimestampUnixNano := sample.TimestampsUnixNano().At(t)
						sampleTimestampNano := time.Unix(0, int64(sampleTimestampUnixNano))
						fmt.Printf("  Timestamp[%d]: %d (%s)\n", t,
							sampleTimestampUnixNano,
							sampleTimestampNano)
					}

					if config.ExportSampleAttributes {
						sampleAttrs := sample.AttributeIndices()
						for n := 0; n < sampleAttrs.Len(); n++ {
							attr := attributeTable.At(int(sampleAttrs.At(n)))
							fmt.Printf("  %s: %s\n", stringTable.At(int(attr.KeyStrindex())), attr.Value().AsString())
						}
						fmt.Println("---------------------------------------------------")
					}

					profileLocationsIndices := pd.Dictionary().StackTable().At(int(sample.StackIndex())).LocationIndices()

					if config.ExportStackFrames {
						for m := 0; m < profileLocationsIndices.Len(); m++ {
							location := locationTable.At(int(profileLocationsIndices.At(int(m))))
							locationAttrs := location.AttributeIndices()

							unwindType := "unknown"
							for la := 0; la < locationAttrs.Len(); la++ {
								attr := attributeTable.At(int(locationAttrs.At(la)))
								if stringTable.At(int(attr.KeyStrindex())) == "profile.frame.type" {
									unwindType = attr.Value().AsString()
									break
								}
							}

							if len(config.ExportStackFrameTypes) > 0 &&
								!slices.Contains(config.ExportStackFrameTypes, unwindType) {
								continue
							}

							locationLine := location.Lines()
							if locationLine.Len() == 0 {
								filename := "<unknown>"
								if location.MappingIndex() > 0 {
									mapping := mappingTable.At(int(location.MappingIndex()))
									filename = stringTable.At(int(mapping.FilenameStrindex()))
								}
								fmt.Printf("Instrumentation: %s: Function: %#04x, File: %s\n", unwindType, location.Address(), filename)
							}

							for n := 0; n < locationLine.Len(); n++ {
								line := locationLine.At(n)
								function := functionTable.At(int(line.FunctionIndex()))
								functionName := stringTable.At(int(function.NameStrindex()))
								fileName := stringTable.At(int(function.FilenameStrindex()))
								fmt.Printf("Instrumentation: %s, Function: %s, File: %s, Line: %d, Column: %d\n",
									unwindType, functionName, fileName, line.Line(), line.Column())
							}
						}
					}

					fmt.Println("------------------- End Sample --------------------")
				}
				fmt.Println("------------------- End Profile -------------------")
			}
		}

		fmt.Printf("-------------- End Resource Profile ---------------\n\n")
	}
}

func main() {
	log := slog.Default()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer cancel()

	port := flag.Int("port", 4137, "port to listen on")
	flag.Parse()

	var opts []grpc.ServerOption
	s := grpc.NewServer(opts...)
	pprofileotlp.RegisterGRPCServer(s, newProfilesServer(Config{
		ExportResourceAttributes:         true,
		ExportProfileAttributes:          true,
		ExportSampleAttributes:           true,
		ExportStackFrames:                true,
		IgnoreProfilesWithoutContainerID: false,
		FilterSampleTypes:                []string{"events"},
	}))

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Error("error creating listener", slog.Any("error", err.Error()))
		os.Exit(1)
	}

	go func() {
		err = s.Serve(lis)
	}()

	fmt.Println("GRPC server started at ", lis.Addr().String())

	fmt.Println("running...")
	<-ctx.Done()
	fmt.Println("done...")
	s.GracefulStop()
}
