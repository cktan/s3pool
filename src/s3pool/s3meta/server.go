package s3meta

import "errors"


func (p *serverCB) run() {
	for {
		req := <- p.ch
		switch req.command {
		case "GLOB":
			req.reply <- p.glob(req)
		default:
			req.reply <- &replyType{err: errors.New("bad command: " + req.command)}
		}
	}
}
