import React, { useState, useEffect, useRef } from "react";
import ReactDOM from "react-dom/client";
import * as wasm from "quirky-binder-codegen-wasm";
import testQb from "./test.qb";

const Page = () => {
    const [text, setText] = useState(testQb);
    const [qb, setQb] = useState(testQb);
    const [rust, setRust] = useState("");
    const [isValid, setIsValid] = useState(undefined);
    const [highlights, setHighlights] = useState(undefined);
    const [trackPosition, setTrackPosition] = useState(false);

    const inputRef = useRef(null);
    const highlightRef = useRef(null);

    useEffect(() => {
        if (trackPosition) {
            if (inputRef.current) {
                setQb(inputRef.current.value.substring(0, inputRef.current.selectionStart));
            }
        } else {
            setQb(text);
        }
    }, [trackPosition]);

    const refreshScroll = () => {
        setTimeout(() => {
            if (highlightRef.current && inputRef.current) {
                highlightRef.current.scrollTop = inputRef.current.scrollTop;
            }
        }, 100);
    };

    useEffect(() => {
        const value = qb;
        try {
            const newRust = wasm.quirky_binder(value);
            setIsValid(true);
            setRust(newRust);
            setHighlights(value);
            refreshScroll();
        } catch (e) {
            setIsValid(false);
            setRust("");
            if (!e.errors) {
                setHighlights(<div style={{color: "red", fontSize: "2em"}}>{e.toString()}</div>);
                refreshScroll();
                return;
            }
            const errors = e.errors;

            errors.sort(({ span: a }, { span: b }) => {
                if (a.start < b.start) {
                    return -1;
                } else if (a.start > b.start) {
                    return 1;
                } else if (a.end < b.end) {
                    return -1;
                } else if (a.end > b.end) {
                    return 1;
                } else {
                    return 0;
                }
            });
            const data = [];
            let cursor = 0;
            let errorIndex = 0;
            for (let i = 0; i < value.length; ++i) {
                while (errorIndex < errors.length && errors[errorIndex].span.end < i) {
                    errorIndex += 1;
                }
                if (errorIndex < errors.length) {
                    if (errors[errorIndex].span.start == i) {
                        if (cursor < i) {
                            data.push(value.substring(cursor, i));
                            cursor = i;
                        }
                    }
                }
                while (errorIndex < errors.length) {
                    if (errors[errorIndex].span.end == i) {
                        data.push(<span key={`preceding_${errorIndex}`} style={{color: "red"}}>{value.substring(cursor, i)}</span>);
                        data.push(<span key={`error_${errorIndex}`} style={{color: "red", fontSize: "0.8em", verticalAlign: "super", cursor: "pointer"}}>{errorIndex}: {errors[errorIndex].error}</span>);
                        cursor = i;
                        errorIndex += 1;
                    } else {
                        break;
                    }
                }
            }
            while (errorIndex < errors.length && errors[errorIndex].span.end < value.length) {
                errorIndex += 1;
            }
            if (errorIndex < errors.length) {
                if (errors[errorIndex].span.start == value.length) {
                    if (cursor < value.length) {
                        data.push(value.substring(cursor, value.length));
                        cursor = value.length;
                    }
                }
            }
            while (errorIndex < errors.length) {
                if (errors[errorIndex].span.end == value.length) {
                    data.push(<span key={`preceding_${errorIndex}`} style={{color: "red"}}>{value.substring(cursor, value.length)}</span>);
                    data.push(<span key={`error_${errorIndex}`} style={{color: "red", fontSize: "0.8em", verticalAlign: "super", cursor: "pointer"}}>{errorIndex}: {errors[errorIndex].error}</span>);
                    cursor = value.length;
                    errorIndex += 1;
                } else {
                    break;
                }
            }
            if (cursor < value.length) {
                data.push(value.substring(cursor, value.length));
            }
            setHighlights(data);
            refreshScroll();
        }
    }, [qb]);

    return (<>
        <div style={{margin: 20}}>
            <label><input type="checkbox" checked={trackPosition} onChange={e => { setTrackPosition(e.target.checked); }} />Track position</label>
        </div>
        <div style={{width: "100%", display: "flex", height: "70vh", gap: 20}}>
            <textarea
                ref={inputRef}
                style={{
                    flex: 1,
                    width: "100%",
                    height: "auto",
                    margin: 0,
                    border: "2px solid black",
                    borderRadius: "4px",
                    outline: "none",
                    padding: 8,
                    resize: "none",
                }}
                spellCheck={false}
                value={text}
                onChange={e => {
                    setText(e.target.value);
                }}
                onClick={e => {
                    if (trackPosition) {
                        setQb(e.target.value.substring(0, e.target.selectionStart));
                    } else {
                        setQb(e.target.value);
                    }
                }}
                onKeyUp={e => {
                    if (trackPosition) {
                        setQb(e.target.value.substring(0, e.target.selectionStart));
                    } else {
                        setQb(e.target.value);
                    }
                }}
                onScroll={e => {
                    if (highlightRef.current) {
                        highlightRef.current.scrollTop = e.target.scrollTop;
                    }
                }}
            />
            <div
                ref={highlightRef}
                style={{
                    flex: 1,
                    border: isValid === undefined ?
                        "2px solid black" :
                        (isValid ?
                            "2px solid green" :
                            "2px solid red"
                        ),
                    borderRadius: "4px",
                    padding: 8,
                    fontFamily: "monospace",
                    whiteSpace: "pre-wrap",
                    overflow: "auto",
                }}
            >
                {highlights}
            </div>
        </div>
        <div
            style={{
                marginTop: 20,
                border: isValid ? "2px solid blue" : "2px solid #eeeeee",
                borderRadius: "4px",
                padding: 8,
                backgroundColor: "#eeeeee",
                overflow: "auto",
            }}
        >
            <div style={{width: "100%", fontFamily: "monospace", height: "calc(30vh - 124px)", overflow: "auto"}}>{rust}</div>
        </div>
    </>);
};

const root = ReactDOM.createRoot(document.getElementById("reactRoot"));
root.render(<Page />);
