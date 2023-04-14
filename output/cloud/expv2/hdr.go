package expv2

import (
	"math"
	"time"

	"go.k6.io/k6/output/cloud/expv2/pbcloud"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// lowestTrackable represents the minimum value that the histogram tracks.
	// Essentially, it excludes negative numbers.
	// Most of metrics tracked by histograms are durations
	// where we don't expect negative numbers.
	//
	// In the future, we may expand and include them,
	// probably after https://github.com/grafana/k6/issues/763.
	lowestTrackable = 0

	// highestTrackable represents the maximum
	// value that the histogram is able to track with high accuracy (0.1% of error).
	// It should be a high enough
	// and rationale value for the k6 context; 2^30 = 1073741824
	highestTrackable = 1 << 30
)

type histogram struct {
	Buckets            []uint32
	ExtraLowBucket     uint32
	ExtraHighBucket    uint32
	FirstNotZeroBucket uint32
	LastNotZeroBucket  uint32

	Max   float64
	Min   float64
	Sum   float64
	Count uint32
}

// newHistogram creates an HDR histogram of the provided values.
func newHistogram(values []float64) histogram {
	h := histogram{}
	if len(values) < 1 {
		return h
	}

	for i := 0; i < len(values); i++ {
		h.addToBucket(values[i])
	}

	h.trimzeros()
	return h
}

func (h *histogram) addToBucket(v float64) {
	if h.Count == 0 {
		h.Max, h.Min = v, v
	} else {
		if v > h.Max {
			h.Max = v
		}
		if v < h.Min {
			h.Min = v
		}
	}

	h.Count++
	h.Sum += v

	if v > highestTrackable {
		h.ExtraHighBucket++
		return
	}
	if v < lowestTrackable {
		h.ExtraLowBucket++
		return
	}

	index := resolveBucketIndex(v)
	blen := uint32(len(h.Buckets))
	if blen == 0 {
		h.FirstNotZeroBucket = index
		h.LastNotZeroBucket = index
	} else {
		if index < h.FirstNotZeroBucket {
			h.FirstNotZeroBucket = index
		}
		if index > h.LastNotZeroBucket {
			h.LastNotZeroBucket = index
		}
	}

	if index >= blen {
		// TODO: evaluate if a pool can improve
		// expand with zeros up to the required index
		h.Buckets = append(h.Buckets, make([]uint32, index-blen+1)...)
	}
	h.Buckets[index]++
}

// trimzeros removes all buckets that have a zero value
// from the begin and from the end until
// / they find the first not zero bucket.
func (h *histogram) trimzeros() {
	if h.Count < 1 || len(h.Buckets) < 1 {
		return
	}

	// all the counters are set to zero, we can remove all
	if h.FirstNotZeroBucket == 0 && h.LastNotZeroBucket == 0 {
		h.Buckets = []uint32{}
		return
	}

	h.Buckets = h.Buckets[h.FirstNotZeroBucket : h.LastNotZeroBucket+1]
}

// histogramAsProto converts the histogram into the equivalent Protobuf.
func histogramAsProto(h histogram, time time.Time) *pbcloud.TrendHdrValue {
	hval := &pbcloud.TrendHdrValue{
		Time:              timestamppb.New(time),
		MinResolution:     1.0,
		SignificantDigits: 2,
		LowerCounterIndex: h.FirstNotZeroBucket,
		MinValue:          h.Min,
		MaxValue:          h.Max,
		Sum:               h.Sum,
		Count:             h.Count,
		Counters:          h.Buckets,
	}
	if h.ExtraLowBucket > 0 {
		hval.ExtraLowValuesCounter = &h.ExtraLowBucket
	}
	if h.ExtraHighBucket > 0 {
		hval.ExtraHighValuesCounter = &h.ExtraHighBucket
	}
	return hval
}

// resolveBucketIndex returns the relative index
// in the bucket series for the provided value.
func resolveBucketIndex(val float64) uint32 {
	// the lowest trackable value is zero
	// negative number are not expected
	if val < 0 {
		return 0
	}

	upscaled := int32(math.Ceil(val))
	bucketIdx := upscaled

	//# k is a power of 2 closest to 10^precision_points
	//# i.e 2^7  = 128  ~  100 = 10^2
	//#     2^10 = 1024 ~ 1000 = 10^3
	//# f(x) = 3*x + 1 - empiric formula that works for us
	//# since f(2)=7 and f(3)=10
	const k = 7

	// if upscaled_val >= 1<<(k+1) {
	// 256 = 1<<(7+1) = 1<<(7+1)
	if upscaled >= 256 {
		//# Here we use some math to get simple formula
		//# derivation:
		//# let n = msb(u) - most significant digit position
		//# i.e. n = floor(log(u, 2))
		//#   major_bucket_index = n - k + 1
		//#   sub_bucket_index = u>>(n - k) - (1<<k)
		//#   bucket = major_bucket_index << k + sub_bucket_index =
		//#          = (n-k+1)<<k + u>>(n-k) - (1<<k) =
		//#          = (n-k)<<k + u>>(n-k)
		//#
		nk_diff := int32(math.Floor(math.Log2(float64(upscaled >> k))))
		bucketIdx = (nk_diff << k) + (upscaled >> nk_diff)
	}

	return uint32(bucketIdx)
}
