package api

import (
	"errors"
	"time"
)

const nasOriginalWitnessPolicy = "registered-day-presentation-v1"

type nasOriginalWitnessContext struct {
	PolicyVersion      string    `json:"policy_version"`
	RegisteredStartUTC time.Time `json:"registered_start_utc"`
	RegisteredEndUTC   time.Time `json:"registered_end_utc"`
	Timezone           string    `json:"timezone"`
	LocalDate          string    `json:"local_date"`
}
type nasOriginalWitnessProof struct {
	PolicyVersion    string     `json:"policy_version"`
	Status           string     `json:"status"`
	Reason           string     `json:"reason"`
	VideoFrames      int64      `json:"video_frames"`
	StrictFullDecode bool       `json:"strict_full_decode"`
	IntervalsUS      [][2]int64 `json:"intervals_us"`
	FFmpegSHA256     string     `json:"ffmpeg_sha256"`
	FFprobeSHA256    string     `json:"ffprobe_sha256"`
}

func validateNASOriginalWitnessContext(c *nasOriginalWitnessContext) error {
	if c == nil || c.PolicyVersion != nasOriginalWitnessPolicy || c.Timezone == "" || c.RegisteredStartUTC.IsZero() || c.RegisteredEndUTC.IsZero() {
		return errors.New("invalid original witness context")
	}
	loc, err := time.LoadLocation(c.Timezone)
	if err != nil || !c.RegisteredEndUTC.After(c.RegisteredStartUTC) || c.RegisteredEndUTC.Sub(c.RegisteredStartUTC) > 15*time.Minute || c.RegisteredStartUTC.Nanosecond()%1000 != 0 || c.RegisteredEndUTC.Nanosecond()%1000 != 0 {
		return errors.New("invalid original witness registered span")
	}
	if c.RegisteredStartUTC.In(loc).Format("2006-01-02") != c.LocalDate || c.RegisteredEndUTC.Add(-time.Microsecond).In(loc).Format("2006-01-02") != c.LocalDate {
		return errors.New("original witness must belong to one canonical registered date")
	}
	return nil
}
func validateNASOriginalWitness(p *nasOriginalWitnessProof, c *nasOriginalWitnessContext) error {
	if err := validateNASOriginalWitnessContext(c); err != nil {
		return err
	}
	if p == nil || p.PolicyVersion != nasOriginalWitnessPolicy || len(p.Reason) > 500 {
		return errors.New("invalid original witness proof")
	}
	if p.Status == "unverified" {
		if p.Reason == "" || p.VideoFrames != 0 || p.StrictFullDecode || len(p.IntervalsUS) != 0 {
			return errors.New("unverified witness cannot assert presentation coverage")
		}
		return nil
	}
	if p.Status != "verified" || p.Reason != "" || !p.StrictFullDecode || p.VideoFrames < 1 || p.VideoFrames > 120000 || len(p.IntervalsUS) < 1 || len(p.IntervalsUS) > 1024 || int64(len(p.IntervalsUS)) > p.VideoFrames || len(p.FFmpegSHA256) != 64 || !lowerHex(p.FFmpegSHA256) || len(p.FFprobeSHA256) != 64 || !lowerHex(p.FFprobeSHA256) {
		return errors.New("incomplete verified original witness")
	}
	cap := c.RegisteredEndUTC.Sub(c.RegisteredStartUTC).Microseconds()
	var previous int64 = -1
	for _, span := range p.IntervalsUS {
		if span[0] < 0 || span[1] <= span[0] || span[1] > cap || span[0] <= previous {
			return errors.New("original witness intervals must be capped, merged and ordered")
		}
		previous = span[1]
	}
	return nil
}
