// 用法展示
// 上传文件示例：go run main.go --action upload --uploadFilepaths /Users/haixian.luo/test/FtpData/data/abc.pdf
// 下载文件示例：go run main.go --action download --downloadDir /Users/haixian.luo/test/FtpData/download --downloadFilenames abc.pdf
// 列出文件示例：go run main.go --action list

package main

import (
    "FtpClient/common"
    "FtpClient/downloader"
    "FtpClient/uploader"
    "encoding/json"
    "flag"
    "fmt"
    "net/http"
    "os"
    "strings"
    "sync"
    "time"
)

// 定义全局变量
var globalWait sync.WaitGroup   // 等待多个文件上传或下载完

// 定义命令行参数对应的变量
var serverIP = flag.String("serverIP", "127.0.0.1", "服务IP")
var serverPort = flag.Int("serverPort", 800, "服务端口")
var action = flag.String("action", "", "upload, download or list")
var uploadFilepaths = flag.String("uploadFilepaths", "", "上传文件路径,多个文件路径用空格相隔")
var downloadFilenames = flag.String("downloadFilenames", "", "下载文件名")
var downloadDir = flag.String("downloadDir", "/data/lhx/FtpData/download", "下载路径，默认当前目录")

// 上传文件
func uploadFile(uploadFilepath string) {
    defer globalWait.Done()

    var err error = nil

    // 获取文件大小，如果小于等于1M则整个文件上传，否则采用分片方式上传
    filesize := common.GetFileSize(uploadFilepath)
    if filesize <= common.SmallFileSize {
        // 小文件
        err = uploader.UploadFile(uploadFilepath)

    } else {
        // 大文件，进行切片上传
        // 这里需要判断是否是上传到一半的文件，如果是则重新加载上传器，如果不是则重新创建上传器当新文件进行上传
        uloader := uploader.GetUploader(uploadFilepath, common.SliceBytes)
        if uloader == nil {
            fmt.Println("这是一个全新要上传的文件")
            uloader = uploader.NewUploader(uploadFilepath, common.SliceBytes)
        }

        if uloader == nil {
            fmt.Println("创建上传器失败，上传文件失败")
            return
        }

        // 切片方式进行文件上传
        err = uloader.UploadFileBySlice()
    }

    if err != nil {
        fmt.Printf("上传%s文件失败\n", uploadFilepath)
    }
}

// 上传多个文件
func uploadFiles(uploadFilepaths string) {
    // 以空格方式分割要上传的文件
    files := strings.Split(uploadFilepaths, " ")
    for _, file := range files {
        globalWait.Add(1)
        go uploadFile(file)
    }
    globalWait.Wait()
}

// 获取文件基本信息，用以判断是普通类型文件还是切片类型文件
func getFileInfo(filename string) (*common.FileInfo, error) {
    targetUrl := common.BaseUrl + "getFileInfo?filename=" + filename

    req, _ := http.NewRequest("GET", targetUrl, nil)
    resp, err := (&http.Client{}).Do(req)
    if err != nil {
        fmt.Println(err)
        return nil, err
    }
    defer resp.Body.Close()

    var baseInfo common.FileInfo
    err = json.NewDecoder(resp.Body).Decode(&baseInfo)
    if err != nil {
        fmt.Println("获取文件基本失败")
        return nil, err
    }

    return &baseInfo, nil
}

// 下载文件
func downloadFile(filename string, downloadDir string) {
    defer globalWait.Done()

    fileInfo, err := getFileInfo(filename)
    if err != nil {
        fmt.Printf("%s文件下载失败", filename)
        return
    }

    switch fileInfo.Filetype {
    case "normal":
        // 普通文件，直接整个下载
        err := downloader.DownloadFile(filename, downloadDir)
        if err != nil {
            fmt.Printf("%s文件下载失败", filename)
        }
    case "slice":
        // 切片文件
        // 这里需要判断是否是下载到一半的文件，如果是则重新加载下载器，如果不是则重新创建下载器进行下载
        dLoader := downloader.GetDownLoader(filename, downloadDir)
        if dLoader == nil {
            fmt.Printf("%s这是一个全新要下载的文件\n", filename)
            dLoader = downloader.NewDownLoader(filename, downloadDir)
        }
        if dLoader == nil {
            fmt.Printf("%s文件下载失败", filename)
        }
        dLoader.DownloadFileBySlice()

        // 合并分片
        dLoader.MergeDownloadFiles()
    default:
        fmt.Printf("%s未知的文件类型，下载失败\n", filename)
    }
}

// 下载多个文件
func downloadFiles(filePaths string, downloadDir string) {
    if !common.IsDir(downloadDir) {
        fmt.Println("路径不存在", downloadDir)
        os.Exit(-1)
    }

    files := strings.Split(filePaths, " ")
    for _, file := range files {
        globalWait.Add(1)
        go downloadFile(file, downloadDir)
    }
    globalWait.Wait()
}

// listFiles 列出文件列表
func listFiles() {
    targetUrl := common.BaseUrl + "listFiles"

    req, _ := http.NewRequest("GET", targetUrl, nil)
    resp, err := (&http.Client{}).Do(req)
    if err != nil {
        fmt.Println("获取文件列表信息失败", err.Error())
        return
    }
    defer resp.Body.Close()

    var fileinfos common.ListFileInfos
    err = json.NewDecoder(resp.Body).Decode(&fileinfos)
    if err != nil {
        fmt.Println("获取文件列表信息失败")
        return
    }

    fmt.Printf("%s      %s\n", "文件名", "文件大小")
    for _, fileinfo := range fileinfos.Files {
        fmt.Printf("%s      %d\n", fileinfo.Filename, fileinfo.Filesize)
    }
}

func main() {
    startTime := time.Now()
    defer func() {
        fmt.Println("程序运行时间：", time.Since(startTime))
    }()

    // 解析传入的参数
    flag.Parse()

    // 设置基础请求URL值
    common.BaseUrl = fmt.Sprintf("http://%s:%d/", *serverIP, *serverPort)

    switch *action {
    case "upload":
        // 上传文件
        uploadFiles(*uploadFilepaths)
    case "download":
        // 下载文件
        downloadFiles(*downloadFilenames, *downloadDir)
    case "list":
        // 列出文件
        listFiles()
    default:
        fmt.Printf("unknow action: %s\n", *action)
        os.Exit(-1)
    }
}
