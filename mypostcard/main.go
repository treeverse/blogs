package main

import (
	"bytes"
	"embed" // The required package for using go:"embed" directive
	"flag"
	"fmt"
	"image"
	"image/draw"
	"image/png"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/freetype/truetype"
	"golang.org/x/image/font"
	"golang.org/x/image/math/fixed"
)

//go:embed LICENSE
var license string

//go:embed postcard.png
var postcardPNG []byte

//go:embed stamp*.png
var stampsPNG embed.FS

//go:embed ClarendonBT.ttf
var clarendonBT []byte

//go:embed *.html *.css LICENSE
var webUI embed.FS

func main() {
	listenAddr := flag.String("l", ":8000", "Listen address")
	printLicense := flag.Bool("license", false, "Print license")
	flag.Parse()

	if *printLicense {
		fmt.Println(license)
		return
	}

	rand.Seed(time.Now().UnixNano())
	// Serve UI files handler
	http.Handle("/", http.FileServer(http.FS(webUI)))
	// Generate postcard handler
	http.HandleFunc("/postcard", postcardHandler)

	fmt.Println("Listen and serve on", *listenAddr)
	err := http.ListenAndServe(*listenAddr, nil)
	if err != nil {
		fmt.Println("ERROR", err)
	}
}

// postcardHandler http handler parse user input and generate PNG postcard in response by calling `generatePostcard`
func postcardHandler(w http.ResponseWriter, r *http.Request) {
	text := r.FormValue("postcard")
	text = strings.ReplaceAll(text, "\r", "")
	lines := strings.Split(text, "\n")
	postcard := generatePostcard(lines)
	var b bytes.Buffer
	err := png.Encode(&b, postcard)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", strconv.Itoa(len(b.Bytes())))
	_, err = w.Write(b.Bytes())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// generatePostcard creates image from postcard background, draw stamp and `lines` strings using `clarendonBT` font
func generatePostcard(lines []string) image.Image {
	postcardImg, _ := png.Decode(bytes.NewReader(postcardPNG))
	fnt, _ := truetype.Parse(clarendonBT)
	stamps, _ := stampsPNG.ReadDir(".")
	stampName := stamps[rand.Intn(len(stamps))].Name()
	stampFile, _ := stampsPNG.Open(stampName)
	stampImg, _ := png.Decode(stampFile)
	b := postcardImg.Bounds()
	postcard := image.NewRGBA(b)
	draw.Draw(postcard, b, postcardImg, image.Point{0, 0}, draw.Src)
	draw.Draw(postcard, stampImg.Bounds().Add(image.Point{835, 45}), stampImg, image.Point{0, 0}, draw.Over)
	const fontSize = 24
	d := &font.Drawer{
		Dst:  postcard,
		Src:  image.Black,
		Face: truetype.NewFace(fnt, &truetype.Options{Size: fontSize}),
	}
	const lineSpacing = fontSize * 2
	d.Dot = fixed.Point26_6{X: fixed.I(50), Y: fixed.I(275 + fontSize)}
	for _, line := range lines {
		d.DrawString(line)
		d.Dot.X = fixed.I(50)
		d.Dot.Y += fixed.I(lineSpacing)
	}
	return postcard
}
