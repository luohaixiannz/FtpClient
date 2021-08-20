package common

import "time"

// 定义常量
const SmallFileSize 			= 1024*1024     // 小文件大小
const SliceBytes 				= 1024*1024*1   // 分片大小
const UploadRetryChannelNum 	= 100			// 上传的重试通道队列大小
const DownloadRetryChannelNum 	= 100			// 下载的重试通道队列大小
const UploadTimeout 			= 300			// 上传超时时间，单位秒
const DownloadTimeout 			= 300			// 上传超时时间，单位秒
const UpGoroutineMaxNumPerFile  = 10			// 每个上传文件开启的goroutine最大数量
const DpGoroutineMaxNumPerFile  = 10			// 每个下载文件开启的goroutine最大数量

// 定义公共变量
var BaseUrl string

// FileInfo 列出文件元信息
type FileInfo struct {
	Filename    string  // 文件名
	Filesize    int64   // 文件大小
	Filetype    string  // 文件类型（目前有普通文件和切片文件两种）
}

// ListFileInfos 文件列表结构
type ListFileInfos struct {
	Files    []FileInfo
}

// FileMetadata 文件片元数据
type FileMetadata struct {
	Fid             string          // 操作文件ID，随机生成的UUID
	Filesize        int64           // 文件大小（字节单位）
	Filename        string          // 文件名称
	SliceNum        int             // 切片数量
	Md5sum          string          // 文件md5值
	ModifyTime      time.Time       // 文件修改时间
}

type SliceSeq struct {
	Slices  []int   // 需要重传的分片号
}